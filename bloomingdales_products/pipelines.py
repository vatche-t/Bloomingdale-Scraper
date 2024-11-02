import pandas as pd
from loguru import logger
import os

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
        for column in required_columns:
            if column not in df.columns:
                df[column] = None

        # Reorder the DataFrame columns
        df = df[required_columns]

        # Remove duplicates based on 'product_code'
        df = df.drop_duplicates(subset='product_code')

        # Check if files already exist
        csv_path = 'data/bloomingdales_products.csv'
        excel_path = 'data/bloomingdales_products.xlsx'

        if os.path.exists(csv_path):
            existing_df = pd.read_csv(csv_path)
            df = pd.concat([existing_df, df]).drop_duplicates(subset='product_code', keep='last')
        
        if os.path.exists(excel_path):
            existing_df = pd.read_excel(excel_path)
            df = pd.concat([existing_df, df]).drop_duplicates(subset='product_code', keep='last')

        # Save to CSV, overwriting the existing file
        df.to_csv(csv_path, index=False)
        # Save to Excel, overwriting the existing file
        df.to_excel(excel_path, index=False)

        logger.info('Data has been successfully exported to CSV and Excel files without duplicates.')
