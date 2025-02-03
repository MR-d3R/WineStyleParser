import os
import csv
from itertools import islice
import logging

logger = logging.getLogger('Parser')


class DBManager:

    def __init__(self, db_path):
        self.db_path = db_path
        self.fieldnames = [
            "shop", "datetime", "price_reg", 'price_promo', 'article', 'name',
            'category_path'
        ]
        self._initialize_csv_file()

    def _initialize_csv_file(self):
        """
        Инициализирует CSV-файл, создавая заголовки, если файл пустой или не существует
        """
        try:
            # Проверяем, существует ли файл и является ли он пустым
            if not os.path.exists(self.db_path) or os.path.getsize(
                    self.db_path) == 0:
                with open(self.db_path, 'w', newline='',
                          encoding='utf-8') as csvfile:

                    writer = csv.DictWriter(csvfile,
                                            fieldnames=self.fieldnames)
                    writer.writeheader()

        except Exception as e:
            logger.error(f"Error initializing CSV file: {e}")

    def create_products(self, products):
        """
        Добавляет новые продукты в существующий CSV-файл
        
        Args:
            products (List[Product]): Список продуктов для сохранения
        """
        try:
            data = []
            for product in products:
                product_data = {
                    "shop": product.shop,
                    'datetime': product.datetime.strftime('%Y-%m-%d %H:%M:%S'),
                    'price_reg': max(product.prices),
                    'price_promo': min(product.prices),
                    'article': product.article,
                    'name': product.name,
                    'category_path': ""
                }
                data.append(product_data)

            # Определяем заголовки, если они еще не были определены
            if not hasattr(self, 'fieldnames'):
                self.fieldnames = [
                    "shop", "datetime", "price_reg", 'price_promo', 'article',
                    'name', 'category_path'
                ]

            with open(self.db_path, 'a', newline='',
                      encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)

                if csvfile.tell() == 0:
                    writer.writeheader()

                writer.writerows(data)

            return len(data)

        except Exception as e:
            logger.error(f"Error creating products: {e}")
            return 0

    def get_products(self, limit=None):
        with open(self.db_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            if limit is None:
                return list(reader)
            else:
                return list(islice(reader, limit))

    def delete_product(self, article_number):
        try:
            rows = self.get_products()
            with open(self.db_path, 'w', newline='',
                      encoding='utf-8') as csvfile:

                writer = csv.DictWriter(csvfile, fieldnames=self.fieldnames)
                writer.writeheader()

                for row in rows:
                    if row['Article Number'] != article_number:
                        writer.writerow(row)

        except Exception as e:
            logger.error(f"Error deleting product: {e}")
            return False

        return True
