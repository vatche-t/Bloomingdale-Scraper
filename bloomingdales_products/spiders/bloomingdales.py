import scrapy
import pandas as pd
from datetime import datetime
from loguru import logger
import random

# Configure logging with Loguru
logger.add("logs/scraper.log", rotation="1 MB", level="DEBUG")
logger.info("Starting the Bloomingdale's scraper...")

class BloomingdalesSpider(scrapy.Spider):
    """
    This Scrapy spider is designed to scrape product details from the Bloomingdale's website. It navigates through various categories 
    (Women, Men, Kids), extracts product information including brand, product name, full price, discounted price, product URL, and more.
    The extracted data is then saved in JSON format. This spider also handles pagination and ensures that all products within a category 
    are scraped.

    Features:
    - Extracts product details such as brand, product name, prices (full price and discounted price if available), image URLs, and product URLs.
    - Automatically handles pagination to scrape multiple pages within a category.
    - Saves the scraped data in JSON format.
    """
    """
    این اسپایدر Scrapy برای جمع‌آوری اطلاعات محصولات از وب‌سایت Bloomingdale's طراحی شده است. این اسپایدر از طریق دسته‌بندی‌های مختلف 
    (زنان، مردان، کودکان) به حرکت در می‌آید و اطلاعات محصول شامل برند، نام محصول، قیمت کامل، قیمت تخفیف‌خورده، URL محصول و سایر اطلاعات را استخراج می‌کند.
    داده‌های استخراج شده به فرمت JSON ذخیره می‌شوند. این اسپایدر همچنین صفحات مختلف یک دسته‌بندی را به طور خودکار پردازش کرده و اطمینان حاصل می‌کند که 
    همه محصولات آن دسته‌بندی استخراج شوند.

    ویژگی‌ها:
    - اطلاعات محصول مانند برند، نام محصول، قیمت‌ها (قیمت کامل و قیمت تخفیف‌خورده در صورت موجود)، URL تصاویر و URL محصول را استخراج می‌کند.
    - به طور خودکار صفحات مختلف را برای استخراج محصولات در یک دسته‌بندی مدیریت می‌کند.
    - داده‌های استخراج شده را به فرمت JSON ذخیره می‌کند.
    """


    name = "bloomingdales"
    allowed_domains = ["bloomingdales.com"]

    # Main URLs to start scraping
    start_urls = [
        "https://www.bloomingdales.com/shop/womens-apparel?id=2910",  # Women
        "https://www.bloomingdales.com/shop/mens?id=3864",  # Men
        "https://www.bloomingdales.com/shop/kids?id=3866"  # Kids
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
        """Initial parsing logic to extract category links from the main category pages."""
        """
        This function is the initial parsing logic that scrapes category links from the main category pages (Women, Men, Kids).
        Once the category links are found, it triggers the `parse_category` function to scrape product details within those categories.

        Args:
            response (scrapy.http.Response): The response object from Scrapy containing the HTML of the page.

        Yields:
            scrapy.Request: A request for each category URL to be followed and parsed by `parse_category`.
        """
        """
        این تابع منطق اولیه‌ی تجزیه برای استخراج لینک‌های دسته‌بندی از صفحات اصلی دسته‌بندی (زنان، مردان، کودکان) است.
        پس از پیدا کردن لینک‌های دسته‌بندی، تابع `parse_category` را فراخوانی می‌کند تا جزئیات محصولات در آن دسته‌بندی‌ها استخراج شود.

        ورودی‌ها:
            response (scrapy.http.Response): شیء پاسخ از Scrapy که شامل HTML صفحه است.

        خروجی:
            scrapy.Request: یک درخواست برای هر URL دسته‌بندی که توسط `parse_category` پیگیری و پردازش می‌شود.
        """


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

    def parse_category(self, response):
        """Extract products on the page and handle pagination."""
        """
        This function parses the products on each category page. It extracts product details like brand, product name, product URL, image URL,
        and prices (both full price and discounted price if available). It also handles pagination to ensure all products in a category are scraped.

        Args:
            response (scrapy.http.Response): The response object containing the HTML of the category page.

        Yields:
            dict: A dictionary containing product details like brand, product code, price, image URL, etc.
            scrapy.Request: A request to follow the next page for pagination, if available.
        """
        
        """
        این تابع محصولات موجود در هر صفحه‌ی دسته‌بندی را تجزیه می‌کند. اطلاعات محصول مانند برند، نام محصول، URL محصول، URL تصویر،
        و قیمت‌ها (هم قیمت کامل و هم قیمت تخفیف‌خورده در صورت موجود بودن) را استخراج می‌کند. همچنین مدیریت صفحه‌بندی را انجام می‌دهد تا مطمئن شود
        همه محصولات در یک دسته‌بندی استخراج می‌شوند.

        ورودی‌ها:
            response (scrapy.http.Response): شیء پاسخ که شامل HTML صفحه دسته‌بندی است.

        خروجی:
            dict: یک دیکشنری شامل جزئیات محصول مانند برند، کد محصول، قیمت، URL تصویر و غیره.
            scrapy.Request: یک درخواست برای پیگیری صفحه بعدی برای صفحه‌بندی، در صورت موجود بودن.
        """


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
                    'category1_code': response.meta.get('category_1'),
                    'category2_code': response.meta.get('category_2'),
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
        """
        This function extracts both the full price and discounted price (if available) for a product. It checks for the presence of 
        a "Now" price, which indicates a discount, and uses XPath to extract the price details accordingly.

        Args:
            product (scrapy.selector.Selector): The selector object containing the HTML of a single product.

        Returns:
            tuple: A tuple containing two values:
                - full_price (float or None): The full price of the product. 
                - discounted_price (float or None): The discounted price of the product, or None if no discount is available.
        """
        
        """
        این تابع هم قیمت کامل و هم قیمت تخفیف‌خورده (در صورت موجود بودن) را برای یک محصول استخراج می‌کند. ابتدا وجود قیمت "Now" 
        (قیمت تخفیف‌خورده) را بررسی می‌کند و سپس از طریق XPath جزئیات قیمت را استخراج می‌کند.

        ورودی‌ها:
            product (scrapy.selector.Selector): شیء سلکتور که شامل HTML یک محصول خاص است.

        خروجی:
            tuple: یک تاپل شامل دو مقدار:
                - full_price (float or None): قیمت کامل محصول.
                - discounted_price (float or None): قیمت تخفیف‌خورده محصول، یا None اگر تخفیفی موجود نباشد.
        """


        full_price = discounted_price = None

        # First, use XPath to check if a discounted price ("Now") exists
        discount_price_text = product.xpath('.//div[@class="show-percent-off"]/span/span[contains(text(),"Now")]/text()').get()
        
        # If we find a "Now" price, extract both the discounted price and the full price
        if discount_price_text and "Now" in discount_price_text:
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

        # Debugging: Log the extracted prices
        logger.debug(f"Full Price: {full_price}")
        logger.debug(f"Discounted Price: {discounted_price}")

        return full_price, discounted_price

    def convert_price_to_float(self, price_str):
        """Convert price string to float."""
        """
        This function converts a price string (e.g., "$150.00") into a floating-point number. It handles removing dollar signs, commas, 
        and any extra whitespace from the price string.

        Args:
            price_str (str): The price string to be converted.

        Returns:
            float or None: The floating-point value of the price, or None if the conversion fails.
        """

        if not price_str:
            return None
        try:
            price_str = price_str.replace("$", "").replace(",", "").strip()
            return float(price_str.split("-")[0].strip())  # Handle price ranges
        except ValueError:
            logger.error(f"Error converting price to float: {price_str}")
            return None
