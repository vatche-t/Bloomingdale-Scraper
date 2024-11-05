import scrapy
import pandas as pd
import os
import shutil
import time
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from loguru import logger

class BloomingdalesImageScraper(scrapy.Spider):
    name = "bloomingdales_image_scraper"
    allowed_domains = ["bloomingdales.com"]

    custom_settings = {
        'CONCURRENT_REQUESTS': 16,
        'RETRY_TIMES': 5,
        'DOWNLOAD_DELAY': 1.0,
        'AUTOTHROTTLE_ENABLED': True,
        'HTTPCACHE_ENABLED': True,
        'LOG_LEVEL': 'DEBUG'
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Create backup folder if it doesn't exist and save a copy of the current CSV and Excel files
        if not os.path.exists('data/backup'):
            os.makedirs('data/backup')

        if os.path.exists('data/bloomingdales_products.csv'):
            shutil.copy2('data/bloomingdales_products.csv', 'data/backup/bloomingdales_products_backup.csv')
        if os.path.exists('data/bloomingdales_products.xlsx'):
            shutil.copy2('data/bloomingdales_products.xlsx', 'data/backup/bloomingdales_products_backup.xlsx')

        # Load the Excel file
        try:
            self.df = pd.read_excel('data/bloomingdales_products.xlsx')
            logger.info(f"Loaded Excel file with {len(self.df)} total products.")
        except Exception as e:
            logger.error(f"Error loading Excel file: {e}")
            self.df = pd.DataFrame()  # Ensure self.df is always a DataFrame

        # Filter products to scrape based on missing image URLs
        self.products_to_scrape = self.df[self.df['imageurl'].isnull() | self.df['imageurl'].eq('')]
        logger.info(f"Found {len(self.products_to_scrape)} products without an image URL to scrape.")

        # Create updated folder to save the final results if it doesn't exist
        if not os.path.exists('data/updated'):
            os.makedirs('data/updated')

    def start_requests(self):
        base_url = "https://www.bloomingdales.com"

        if self.products_to_scrape.empty:
            logger.warning("No products to scrape. Stopping spider.")
            return

        for index, row in self.products_to_scrape.iterrows():
            product_url = row['itemurl']
            product_code = row['product_code']
            
            logger.debug(f"Processing product: {product_code}, URL: {product_url}")

            if not product_url.startswith('http'):
                product_url = base_url + product_url
                logger.debug(f"Updated relative URL to: {product_url}")

            yield scrapy.Request(
                url=product_url,
                callback=self.parse,
                meta={'index': index, 'product_code': product_code},
                errback=self.handle_error
            )

    def handle_error(self, failure):
        request = failure.request
        product_code = request.meta.get('product_code', 'Unknown')
        logger.error(f"Network error or invalid response for product {product_code}. Details: {failure}")
        if not self.df.empty:
            self.df.at[request.meta['index'], 'imageurl'] = None

    def parse(self, response):
        index = response.meta['index']
        product_code = response.meta['product_code']

        selectors = [
            'picture.main-picture > img::attr(src)',
            'picture.main-picture > source::attr(srcset)',
            '//picture[@class="main-picture"]/img/@src',
            '//picture[@class="main-picture"]/source[1]/@srcset',
            '//div[@class="picture-container"]/picture/source[@media="(max-width: 599px)"]/@srcset',
            '//div[@class="picture-container"]/picture/source[@media="(min-width: 600px and max-width: 1023px)"]/@srcset',
            '//div[@class="picture-container"]/picture/source[@media="(min-width: 1024px) and (max-width: 1279px)"]/@srcset',
            '//div[@class="picture-container"]/picture/source[@media="(min-width: 1280px) and (max-width: 1599px)"]/@srcset',
            '//div[@class="picture-container"]/picture/source[@media="(min-width: 1600px)"]/@srcset',
        ]
        image_urls = self.extract_images(response, selectors)

        if not image_urls:
            logger.warning(f"No image URLs found for product {product_code}. Retrying after 10 seconds...")
            time.sleep(10)
            image_urls = self.extract_images(response, selectors)

        if not image_urls:
            logger.warning(f"Still no image URLs found for product {product_code} after retry.")

        if not self.df.empty:
            self.df.at[index, 'imageurl'] = ', '.join(image_urls) if image_urls else None

    def extract_images(self, response, selectors):
        image_urls = []
        try:
            for selector in selectors:
                if '::attr' in selector:
                    image_url = response.css(selector).get()
                else:
                    image_url = response.xpath(selector).get()

                if image_url:
                    logger.info(f"Image URL found: {image_url}")
                    image_urls.append(image_url)
        except Exception as e:
            logger.error(f"Error extracting images: {e}")
        return image_urls

    def closed(self, reason):
        if not self.df.empty:
            try:
                updated_csv_path = 'data/updated/bloomingdales_products_updated.csv'
                updated_excel_path = 'data/updated/bloomingdales_products_updated.xlsx'

                self.df.to_csv(updated_csv_path, index=False)
                self.df.to_excel(updated_excel_path, index=False)

                logger.info(f"Scraping complete. Data saved to {updated_csv_path} and {updated_excel_path}.")
            except Exception as e:
                logger.error(f"Error saving updated files: {e}")
        else:
            logger.error("DataFrame is empty, nothing to save.")

        # Check for and handle missing URLs
        self.check_missing_urls_and_retry()

    def check_missing_urls_and_retry(self):
        try:
            updated_df = pd.read_excel('data/updated/bloomingdales_products_updated.xlsx')
            missing_image_urls_df = updated_df[updated_df['imageurl'].isnull() | updated_df['imageurl'].eq('')]

            if not missing_image_urls_df.empty:
                logger.warning(f"Found {len(missing_image_urls_df)} products with missing image URLs after first run. Retrying...")
                self.products_to_scrape = missing_image_urls_df
                process.crawl(BloomingdalesImageScraper)
                process.start(stop_after_crawl=False)
            else:
                logger.info("All products have image URLs. No need to retry.")
        except Exception as e:
            logger.error(f"Error checking or retrying missing URLs: {e}")

if __name__ == "__main__":
    process = CrawlerProcess(settings={
        'LOG_LEVEL': 'DEBUG',
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    })

    dispatcher.connect(lambda: process.stop(), signal=signals.spider_closed)
    process.crawl(BloomingdalesImageScraper)
    process.start()
