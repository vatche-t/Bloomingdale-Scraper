# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


import pandas as pd
from loguru import logger

class BloomingdalesExcelPipeline:
    def __init__(self):
        self.items = []

    def process_item(self, item, spider):
        # Append each item to the list
        self.items.append(item)
        return item

    def close_spider(self, spider):
    # Convert the items to a DataFrame
        df = pd.DataFrame(self.items)

        # Ensure all required columns exist, fill missing columns with None (or any default value)
        required_columns = [
            'website_name', 'competence_date', 'brand', 'product_code', 'country_code', 'currency_code',
            'full_price', 'price', 'category1_code', 'category2_code', 'category3_code', 'title', 'imageurl', 'itemurl'
        ]

        # Add missing columns with None if they don't exist in the DataFrame
        for column in required_columns:
            if column not in df.columns:
                df[column] = None

        # Reorder the DataFrame columns
        df = df[required_columns]

        # Export to Excel
        df.to_excel('bloomingdales_products.xlsx', index=False)
        logger.info('Data has been successfully exported to bloomingdales_products.xlsx')

