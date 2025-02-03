import json
import traceback
import logging

from db_manager import DBManager
from models import Product
from parsing.parsing_processor import ParsingProcessor

# Настройка логирования
logger = logging.getLogger('Parser')
handler = logging.FileHandler('WineStyleParser.log', encoding="utf-8")
handler.setFormatter(
    logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class WineStyleParser:
    """
    Класс для парсинга сайта WineStyle
    """

    def __init__(self, base_url: str, db_manager: DBManager,
                 config_path) -> None:
        """Инициализация парсера с настройками из конфигурационного файла и базой данных.

        Args:
            db_manager (DBManager): Путь CSV файлу для хранения данных
            config_path (str): Путь к файлу конфигурации
        """
        self.base_url = base_url
        self.db_manager = db_manager
        self.city = ""
        self.max_threads = 1
        self.max_categories = 1000

        self._load_config(config_path)
        self.parsing_processor = ParsingProcessor(base_url, logger,
                                                  config_path)
        # self.api_processor = ProductsProcessor(logger, config_path)

        logger.info(f"Парсер инициализирован для города {self.city}")

    def _load_config(self, config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as config_file:
                res_json = json.load(config_file)

            self.city = res_json.get('city', "")
            self.max_threads = res_json.get('threads', 1)
            self.max_categories = res_json.get('max_categories', 1000)

            logger.info(f"Конфигурация загружена из {config_path}")

        except Exception as e:
            logger.error(f"Ошибка при загрузке конфигурации: {e}")
            raise

    def Parse(self):
        logger.info("Начало парсинга")
        # categories_links = self.parsing_processor.get_catalogue_categories()
        # logger.info(f"Найдено категорий: {categories_links}")
        products_list = self.parsing_processor.process_category(
            "https://winestyle.ru/promo/")
        for pr in products_list:
            logger.info(f"""
ПРОДУКТ: {pr.name}
АРТИКУЛЬ: {pr.article}
ЦЕНЫ: {pr.prices}
ВРЕМЯ: {pr.datetime}
--------------------------------------------
""")


def main():
    try:
        base_url = "https://winestyle.ru/"
        db_manager = DBManager("products.csv")
        parser = WineStyleParser(base_url, db_manager, "config.json")
        parser.Parse()
    except Exception as e:
        logger.error(f"Критическая ошибка при работе парсера: {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()
