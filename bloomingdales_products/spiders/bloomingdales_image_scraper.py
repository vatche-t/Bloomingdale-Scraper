import scrapy
import pandas as pd
import time
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from loguru import logger

class BloomingdalesImageScraper(scrapy.Spider):
    name = "bloomingdales_image_scraper"
    allowed_domains = ["bloomingdales.com"]

    custom_settings = {
        'CONCURRENT_REQUESTS': 16,       # Moderate concurrency to balance load
        'RETRY_TIMES': 5,                # Retry failed requests up to 5 times
        'DOWNLOAD_DELAY': 1.0,           # Add a longer delay to ensure page load
        'AUTOTHROTTLE_ENABLED': True,    # Enable Scrapy's built-in throttling
        'HTTPCACHE_ENABLED': True,       # Enable caching to reuse page content
        'LOG_LEVEL': 'DEBUG'             # Set log level to debug for detailed output
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        try:
            self.df = pd.read_excel('bloomingdales_products.xlsx')
            logger.info(f"Loaded Excel file with {len(self.df)} total products.")
        except Exception as e:
            logger.error(f"Error loading Excel file: {e}")
            return
        
        self.products_to_scrape = self.df[self.df['imageurl'].isnull() | self.df['imageurl'].eq('')]
        logger.info(f"Found {len(self.products_to_scrape)} products without an image URL to scrape.")
        
        if self.products_to_scrape.empty:
            logger.warning("No products with missing image URLs. Nothing to scrape.")
        else:
            logger.info(f"Preparing to scrape {len(self.products_to_scrape)} products.")

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
        self.df.at[request.meta['index'], 'imageurl'] = None

    def parse(self, response):
        index = response.meta['index']
        product_code = response.meta['product_code']

        # Initial selectors for image scraping
        selectors = [
            'picture.main-picture > img::attr(src)',
            'picture.main-picture > source::attr(srcset)',
            '//picture[@class="main-picture"]/img/@src',
            '//picture[@class="main-picture"]/source[1]/@srcset',
            '//div[@class="picture-container"]/picture/source[@media="(max-width: 599px)"]/@srcset',
            '//div[@class="picture-container"]/picture/source[@media="(min-width: 600px) and (max-width: 1023px)"]/@srcset',
            '//div[@class="picture-container"]/picture/source[@media="(min-width: 1024px) and (max-width: 1279px)"]/@srcset',
            '//div[@class="picture-container"]/picture/source[@media="(min-width: 1280px) and (max-width: 1599px)"]/@srcset',
            '//div[@class="picture-container"]/picture/source[@media="(min-width: 1600px)"]/@srcset',
        ]

        # Retry mechanism
        image_urls = self.extract_images(response, selectors)
        
        # If no images were found, wait for 10 seconds and retry
        if not image_urls:
            logger.warning(f"No image URLs found for product {product_code}. Retrying after 10 seconds...")
            time.sleep(10)
            image_urls = self.extract_images(response, selectors)

        if not image_urls:
            logger.warning(f"Still no image URLs found for product {product_code} after retry.")

        # Save all found image URLs as a comma-separated string
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
        try:
            self.df.to_excel('bloomingdales_products_updated.xlsx', index=False)
            logger.info("Scraping complete. Data saved to bloomingdales_products_updated.xlsx.")
        except Exception as e:
            logger.error(f"Error saving updated Excel file: {e}")

if __name__ == "__main__":
    process = CrawlerProcess(settings={
        'LOG_LEVEL': 'DEBUG',
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    })
    
    dispatcher.connect(lambda: process.stop(), signal=signals.spider_closed)
    process.crawl(BloomingdalesImageScraper)
    process.start()
