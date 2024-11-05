import scrapy
import pandas as pd
from datetime import datetime
from loguru import logger
import os
import random
import re  # Import regex for text parsing

# Configure logging with Loguru
logger.add("logs/scraper.log", rotation="1 MB", level="DEBUG")
logger.info("Starting the Bloomingdale's scraper...")

class BloomingdalesSpider(scrapy.Spider):
    name = "bloomingdales"
    allowed_domains = ["bloomingdales.com"]

    # Main URLs to start scraping (Designer Brands only)
    start_urls = [
        "https://www.bloomingdales.com/shop/all-designers?id=1001351"  # Designers
    ]

    custom_settings = {
        'DOWNLOAD_DELAY': random.uniform(1, 3),  # Control the number of concurrent requests
        'FEEDS': {
            'data/bloomingdales_products.csv': {
                'format': 'csv',
                'encoding': 'utf8',
                'store_empty': False,
                'indent': 4,
            },
        },
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failed_image_products = []  # Store products with failed image URLs
        self.all_scraped_items = []  # Store all scraped items for later processing
        dispatcher.connect(self.spider_closed, signals.spider_closed)  # Connect signal to close spider

    def parse(self, response):
        """Initial parsing logic to extract brand links."""
        brand_links = response.xpath('//div[@class="brand-items-grid"]//ul/li/a/@href').getall()
        brand_names = response.xpath('//div[@class="brand-items-grid"]//ul/li/a/text()').getall()

        if not brand_links:
            logger.warning(f"No brand links found on {response.url}")
        else:
            logger.info(f"Found {len(brand_links)} brand links on {response.url}")

        for link, brand_name in zip(brand_links, brand_names):  # Limit to 5 for testing
            logger.info(f"Scraping brand: {brand_name}")
            yield response.follow(link, self.parse_designer_brand, meta={'brand_name': brand_name})

    def parse_designer_brand(self, response):
        """Extract products for designer brands and handle pagination."""
        brand_name = response.meta.get('brand_name')

        # Extract total number of products from the page (for logging purposes only)
        total_products_text = response.css('#app-wrapper > div > div:nth-child(3) > div.results-found-message.total-results-found > div > span::text').get()
        
        # Clean the extracted text to get only the number of products (e.g., remove '(26 items)')
        if total_products_text:
            total_products_cleaned = re.sub(r'\D', '', total_products_text)  # Remove non-numeric characters
            total_products = int(total_products_cleaned) if total_products_cleaned.isdigit() else 0
        else:
            total_products = 0

        # Process each product on the brand page
        for product in product_elements:
            product_url = product.css('div.product-description.margin-top-xxs div:nth-child(1) a::attr(href)').get()
            product_name = product.css('div.product-description.margin-top-xxs div:nth-child(1) a div.product-name::text').get()

            # Check if the "Best Seller" text exists inside the specific class
            bestseller_selector = product.css('div.eyebrow.flexText::text').get()
            best_seller_status = True if bestseller_selector and "Best Seller" in bestseller_selector else False

            # Extract the star rating and review count from fieldset's aria-label
            rating_info = product.css('div.reviewlet-spacing div fieldset::attr(aria-label)').get()
            if rating_info:
                stars, reviews = self.extract_rating_and_reviews(rating_info)
            else:
                stars, reviews = None, None

            # Use CSS for the image URL (First attempt)
            image_url = product.css('div.v-scroller ul li.active img::attr(data-src)').get()

            # If image URL is missing, try different selectors (Fallbacks)
            if not image_url:
                image_url = self.extract_image_url(product, response)

            if not image_url:
                image_url = product.css('div.v-scroller ul li.active picture source:nth-child(1)::attr(srcset)').get()

            if not image_url:
                image_url = product.css('div.v-scroller ul li.active img::attr(src)').get()

            if not image_url:
                logger.warning(f"No image URL found for product: {product_name} at {product_url}")
            else:
                logger.info(f"Found image URL for product: {product_name} -> {image_url}")

            # Extract price details using get_price method
            full_price, discounted_price = self.get_price(product)
            product_code = product_url.split("?ID=")[1].split("&")[0] if product_url else None

            if product_url:
                item = {
                    'website_name': 'www.bloomingdales.com',
                    'competence_date': datetime.now().strftime('%Y-%m-%d'),
                    'brand': brand_name,
                    'product_code': product_code,
                    'country_code': 'USA',
                    'currency_code': 'USD',
                    'full_price': full_price if full_price else None,
                    'price': discounted_price if discounted_price else None,
                    'category1_code': stars,  # Star rating
                    'category2_code': reviews,  # Number of reviews
                    'category3_code': best_seller_status,  # Bestseller status (True/False)
                    'title': product_name.strip() if product_name else None,
                    'imageurl': image_url,
                    'itemurl': response.urljoin(product_url)
                }
                self.all_scraped_items.append(item)
                yield item

        # Pagination logic: ensure the spider attempts to scrape up to 10 pages
        current_page = response.meta.get('current_page', 1)
        if current_page < 7:  # Scrape up to 10 pages
            next_page = current_page + 1
            next_page_url = re.sub(r'(buy/[^?]+)(\?)', rf'\1/Pageindex/{next_page}\2', response.url)

            logger.info(f"Scraping page {next_page} for brand {brand_name}")
            yield response.follow(next_page_url, self.parse_designer_brand, meta={'brand_name': brand_name, 'current_page': next_page})
        else:
            logger.info(f"Reached the maximum of 7 pages for {brand_name}. Moving to the next brand.")

    def extract_rating_and_reviews(self, rating_text):
        """Extract star rating and review count from the aria-label text."""
        pattern = re.compile(r"Rated (\d+\.?\d*) stars with (\d+) reviews")
        match = pattern.search(rating_text)
        if match:
            stars = float(match.group(1))
            reviews = int(match.group(2))
            return stars, reviews
        return None, None

    def get_price(self, product):
        full_price = discounted_price = None
        discount_price_text = product.xpath('.//div[@class="show-percent-off"]/span/span[contains(text(),"Now") or contains(text(),"Sale")]/text()').get()
        if discount_price_text:
            discounted_price_xpath = product.xpath('.//div[@class="show-percent-off"]/span[1]/text()').get()
            discounted_price = self.convert_price_to_float(discounted_price_xpath)
            full_price_xpath = product.xpath('.//div[@class="pricing"]//span[contains(@class,"price-strike")]/text()').get()
            full_price = self.convert_price_to_float(full_price_xpath)
        else:
            full_price_xpath = product.xpath('.//div[@class="pricing"]//span[contains(@class,"price-reg")]/text()').get()
            full_price = self.convert_price_to_float(full_price_xpath)
        return full_price, discounted_price

    def convert_price_to_float(self, price_str):
        if not price_str:
            return None
        try:
            price_str = price_str.replace("$", "").replace(",", "").strip()
            return float(price_str.split("-")[0].strip())
        except ValueError:
            logger.error(f"Error converting price to float: {price_str}")
            return None
      
        

    def spider_closed(self, spider):
        """Runs when the spider is closed."""
        logger.info("Spider closed. Starting post-scrape tasks...")

        # Load the data from CSV, deduplicate, and save again
        df = pd.read_csv('data/bloomingdales_products.csv')

        # Remove duplicates based on 'product_code'
        df = df.drop_duplicates(subset='product_code')

        # Overwrite CSV and Excel files
        df.to_csv('data/bloomingdales_products.csv', index=False)
        df.to_excel('data/bloomingdales_products.xlsx', index=False)

        logger.info("CSV and Excel files saved successfully without duplicates.")