import scrapy
import pandas as pd
from datetime import datetime
from loguru import logger
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
            'data/bloomingdales_products.json': {
                'format': 'json',
                'encoding': 'utf8',
                'store_empty': False,
                'indent': 4,
            },
        },
    }

    def parse(self, response):
        """Initial parsing logic to extract brand links."""
        
        # Handle designer brands
        brand_links = response.xpath('//div[@class="brand-items-grid"]//ul/li/a/@href').getall()
        brand_names = response.xpath('//div[@class="brand-items-grid"]//ul/li/a/text()').getall()

        if not brand_links:
            logger.warning(f"No brand links found on {response.url}")
        else:
            logger.info(f"Found {len(brand_links)} brand links on {response.url}")

        for link, brand_name in zip(brand_links, brand_names):
            logger.info(f"Scraping brand: {brand_name}")
            yield response.follow(link, self.parse_designer_brand, meta={'brand_name': brand_name})

    def parse_designer_brand(self, response):
        """Extract products for designer brands and handle pagination."""
        
        brand_name = response.meta.get('brand_name')
        logger.info(f"Scraping products for brand {brand_name}")

        # Adjusting the CSS selector for designer product extraction
        product_elements = response.css('#app-wrapper > div > div:nth-child(3) > ul > li')
        
        if not product_elements:
            logger.warning(f"No products found for brand {brand_name} on {response.url}")
        else:
            logger.info(f"Found {len(product_elements)} products for brand {brand_name}")

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

            # Use CSS for the image URL
            image_url = product.css('div.v-scroller ul li.active img::attr(data-src)').get()

            # If image URL is missing, try the fallback selector
            if not image_url:
                image_url = product.css('div.v-scroller ul li.active picture source:nth-child(2)::attr(srcset)').get()

            # Extract price details using get_price method
            full_price, discounted_price = self.get_price(product)

            # Extract Product ID correctly
            product_code = product_url.split("?ID=")[1].split("&")[0] if product_url else None

            # Create item for pipeline
            if product_url:
                yield {
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

        # Handle pagination for the brand's product pages
        next_page = response.css('#canvas > div.pagination-wrapper nav > ul.pagination > li:nth-child(3) > a::attr(href)').get()
        if next_page:
            logger.info(f"Scraping next page for brand {brand_name}")
            yield response.follow(next_page, self.parse_designer_brand, meta={'brand_name': brand_name})
        else:
            logger.info(f"No more pages to scrape for brand {brand_name}")

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
        """Extract full price and discounted price from product element using both CSS and XPath."""
        
        full_price = discounted_price = None

        # First, use XPath to check if a discounted price ("Now" or "Sale") exists
        discount_price_text = product.xpath('.//div[@class="show-percent-off"]/span/span[contains(text(),"Now") or contains(text(),"Sale")]/text()').get()
        
        # If we find a "Now" or "Sale" price, extract both the discounted price and the full price
        if discount_price_text:
            # Extract the discounted price from the sibling span
            discounted_price_xpath = product.xpath('.//div[@class="show-percent-off"]/span[1]/text()').get()
            if discounted_price_xpath:
                discounted_price = self.convert_price_to_float(discounted_price_xpath)

            # Now, extract the full price (strikethrough price) using a different XPath
            full_price_xpath = product.xpath('.//div[@class="pricing"]//span[contains(@class,"price-strike")]/text()').get()
            if full_price_xpath:
                full_price = self.convert_price_to_float(full_price_xpath)
        else:
            # If no discounted price, get the regular full price
            full_price_xpath = product.xpath('.//div[@class="pricing"]//span[contains(@class,"price-reg")]/text()').get()
            if full_price_xpath:
                full_price = self.convert_price_to_float(full_price_xpath)

        logger.debug(f"Full Price: {full_price}, Discounted Price: {discounted_price}")

        return full_price, discounted_price

    def convert_price_to_float(self, price_str):
        """Convert price string to float."""
        if not price_str:
            return None
        try:
            price_str = price_str.replace("$", "").replace(",", "").strip()
            return float(price_str.split("-")[0].strip())  # Handle price ranges
        except ValueError:
            logger.error(f"Error converting price to float: {price_str}")
            return None
