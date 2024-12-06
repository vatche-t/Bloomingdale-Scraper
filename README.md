# Bloomingdales Scrapy Project

This Scrapy project is designed to scrape product information from Bloomingdale's website, including product details such as price, brand, product ID, and more, and save the data in an Excel file.

## Project Structure

The directory structure of this project is as follows:

```
bloomingdales_products/
│
├── spiders/
│   ├── data/                    # Directory for storing scraped data (e.g., Excel, JSON)
│   ├── logs/                    # Directory for storing logs
│   ├── __init__.py              # Required for Python package
│   ├── bloomingdales.py         # The main spider for scraping Bloomingdale's website
│
├── __init__.py                  # Required for Python package
├── items.py                     # Defines the data structure for scraped items
├── middlewares.py               # Scrapy middlewares (if needed)
├── pipelines.py                 # Item pipeline for processing scraped items
├── settings.py                  # Project settings (e.g., download delay, pipeline settings)
├── scrapy.cfg                   # Scrapy project configuration file
```

## Setup and Installation

1. **Clone the Repository**:
   If you're cloning this repository, use the following command:

   ```bash
   git clone <repository-url>
   cd bloomingdales_products
   ```

2. **Install Required Dependencies**:
   Install Scrapy and other dependencies using `pip`:

   ```bash
   pip install scrapy pandas loguru
   ```

3. **Project Structure**:
   - `spiders/`: Contains the main spider script and directories for storing scraped data and logs.
   - `items.py`: Defines the fields for the items you are scraping.
   - `middlewares.py`: Contains any custom middlewares (optional).
   - `pipelines.py`: Contains the logic for saving the data to an Excel file.
   - `settings.py`: Configure Scrapy settings, such as download delay, pipelines, etc.
   - `scrapy.cfg`: Scrapy configuration file.

## How to Run the Scraper

To start the scraper, use the following command:

```bash
scrapy crawl bloomingdales
```

This will start the Bloomingdale's spider and begin scraping the product data. The scraped data will be stored in the `data/` directory as an Excel file (`bloomingdales_products.xlsx`).

## Features

- **Scrape Multiple Categories**: The spider scrapes data from multiple categories, including Women's, Men's, and Kids' apparel.
- **Extract Product Information**: The scraper extracts important information such as:
  - Product name
  - Brand
  - Full price
  - Discounted price (if available)
  - Product ID
  - Product URL
  - Image URL
- **Pagination Handling**: The spider automatically follows pagination links to scrape data from multiple pages.
- **Excel Output**: The scraped data is saved in an Excel file using Pandas.

## Detailed Breakdown of Files

### 1. `bloomingdales.py`
This is the main spider that scrapes data from the Bloomingdale's website. It handles:
- Scraping product data from the categories listed in `start_urls`.
- Navigating pagination.
- Extracting product details such as brand, price, and product URLs.

### 2. `pipelines.py`
The pipeline processes the scraped items and saves them to an Excel file. It uses Pandas for creating DataFrames and exporting them to Excel.

### 3. `settings.py`
The Scrapy settings file, where important configurations such as pipelines, download delays, and output formats are defined.

### 4. `items.py`
Defines the structure of the scraped items. Although not used explicitly in the spider, it's useful for maintaining a consistent structure of the scraped data.

## Logs

Logs are stored in the `logs/` directory. Each time the spider runs, a log file is generated, helping you track the scraping process and troubleshoot errors.

## Data Output

The scraped data is saved in the `data/` folder in an Excel file format (`bloomingdales_products.xlsx`).

## Example Usage

Here’s an example of how you can start the spider and scrape data:

```bash
scrapy crawl bloomingdales
```

After the spider finishes, check the `data/` directory for the output Excel file.

## Troubleshooting

1. **No Data or Missing Fields**: If you're not getting the expected data, check the logs in the `logs/` directory. The log files will give detailed information about the scraping process and any errors.
2. **Settings Adjustments**: You can tweak the `settings.py` file for adjusting the download delay, enabling/disabling middlewares, and configuring the pipelines.

## Contribution

Feel free to contribute to this project by submitting issues or pull requests. Any improvements or additional features are welcome!



Last updated on: 2024-12-05

Last updated on: 2024-12-06