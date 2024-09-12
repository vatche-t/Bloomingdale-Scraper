import scrapy
import pandas as pd
from datetime import datetime
from loguru import logger
import random

# Configure logging with Loguru
logger.add("logs/scraper.log", rotation="1 MB", level="DEBUG")
logger.info("Starting the Bloomingdale's scraper...")

class BloomingdalesSpider(scrapy.Spider):
    name = "bloomingdales"
    allowed_domains = ["bloomingdales.com"]

    # Main URLs to start scraping
    start_urls = [
        "https://www.bloomingdales.com/shop/womens-apparel?id=2910",  # Women
        "https://www.bloomingdales.com/shop/mens?id=3864",  # Men
        "https://www.bloomingdales.com/shop/kids?id=3866",  # Kids
        "https://www.bloomingdales.com/shop/all-designers?id=1001351"  # Designers
    ]

    custom_settings = {
        'DOWNLOAD_DELAY': random.uniform(1, 3),  # Delay between requests
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
        """Initial parsing logic to extract category and brand links."""
        
        if "all-designers" in response.url:
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

        else:
            # Regular category extraction (Women, Men, Kids)
            category = response.xpath("//title/text()").get().strip().split(' ')[0]
            category_links = response.css(
                "#app-wrapper > div > div.nav-padding-top.grid-x.grid-margin-x.middle-container > div.cell.large-3.small-12.medium-12 > div:nth-child(2) > div > div > ul > li > a"
            )

            if not category_links:
                logger.warning(f"No category links found on {response.url}")
            else:
                logger.info(f"Found {len(category_links)} category links on {response.url}")

            for link in category_links:
                category_name = link.css("::text").get().strip()
                category_url = link.css("::attr(href)").get()

                if "Shop All" not in category_name and "all-women" not in category_url:
                    logger.info(f"Found category: {category_name} - {category_url}")
                    yield response.follow(category_url, self.parse_category, meta={'category_1': category, 'category_2': category_name})

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

        for product in product_elements:
            product_url = product.css('div.product-description.margin-top-xxs div:nth-child(1) a::attr(href)').get()
            product_name = product.css('div.product-description.margin-top-xxs div:nth-child(1) a div.product-name::text').get()
            brand = brand_name  # The brand name is passed through meta, and doesn't need to be scraped

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
                    'brand': brand,
                    'product_code': product_code,
                    'country_code': 'USA',
                    'currency_code': 'USD',
                    'full_price': full_price if full_price else None,
                    'price': discounted_price if discounted_price else None,
                    'category1_code': "Designers",
                    'category2_code': brand_name,
                    'category3_code': None,  # Placeholder for category 3 code, adjust if needed
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

    def parse_category(self, response):
        """Extract products on the page and handle pagination."""
        
        category_1 = response.meta.get('category_1')
        category_2 = response.meta.get('category_2')

        product_elements = response.css('#canvas > ul > li')
        logger.info(f"Found {len(product_elements)} products in category {category_2}")

        for product in product_elements:
            product_url = product.css('div > div > a::attr(href)').get()
            brand = product.css('div.product-brand.heavy::text').get()
            product_name = product.css('div.product-name::text').get()

            # Use the provided selector for the image URL
            image_url = product.css('div.v-scroller ul li.active img::attr(data-src)').get()

            # Extract price details using get_price method
            full_price, discounted_price = self.get_price(product)

            # Extract Product ID correctly
            product_code = product_url.split("?ID=")[1].split("&")[0] if product_url else None

            # Create item for pipeline
            if product_url:
                yield {
                    'website_name': 'www.bloomingdales.com',
                    'competence_date': datetime.now().strftime('%Y-%m-%d'),
                    'brand': brand,
                    'product_code': product_code,
                    'country_code': 'USA',
                    'currency_code': 'USD',
                    'full_price': full_price if full_price else None,
                    'price': discounted_price if discounted_price else None,
                    'category1_code': category_1,
                    'category2_code': category_2,
                    'category3_code': None,  # Placeholder for the category 3 code, adjust if needed
                    'title': product_name.strip() if product_name else None,
                    'imageurl': image_url,
                    'itemurl': response.urljoin(product_url)
                }

        # Handle pagination
        next_page = response.css('#canvas > div.pagination-wrapper nav > ul.pagination > li:nth-child(3) > a::attr(href)').get()
        if next_page:
            yield response.follow(next_page, self.parse_category, meta={'category_1': category_1, 'category_2': category_2})
        else:
            logger.info(f"No more pages to scrape for category {category_2}")

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
