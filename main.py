import json
import traceback
import logging

from db_manager import DBManager
from models import Product
from parsing.parsing_processor import ParsingProcessor
from browser_emu.emulator import Emulator

# Настройка логирования
logger = logging.getLogger('Parser')
handler = logging.FileHandler('WineStyleParser.log', encoding="utf-8")
handler.setFormatter(
    logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

emulator_logger = logging.getLogger('Emulator')
emulator_handler = logging.FileHandler('BrowserEmulator.log', encoding="utf-8")
emulator_handler.setFormatter(
    logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
emulator_logger.addHandler(emulator_handler)
emulator_logger.setLevel(logging.INFO)


class WineStyleParser:
    """
    Класс для парсинга сайта WineStyle
    """

    def __init__(self, base_url: str, db_manager: DBManager,
                 config_path) -> None:
        """Инициализация парсера с настройками из конфигурационного файла и базой данных.

        Args:
            db_manager (DBManager): Объект DBManager
            config_path (str): Путь к файлу конфигурации
        """
        self.base_url = base_url
        self.cat_page_url = base_url
        self.db_manager = db_manager
        self.city = ""
        self.address = ""
        self.parse_categpries = False
        self.max_threads = 1
        self.page_threads = 1
        self.max_categories = 1000
        self.max_pages = 1000
        self.config_path = config_path

        self._load_config(config_path)
        self.browser_emulator = Emulator(emulator_logger, config_path)

        logger.info(f"Парсер инициализирован для города {self.city}")

    def _load_config(self, config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as config_file:
                res_json = json.load(config_file)

            self.city = res_json.get('city', "")
            self.address = res_json.get('address', "")
            self.parse_categpries = res_json.get('parse_categories', False)
            self.max_threads = res_json.get('threads', 1)
            self.max_categories = res_json.get('max_categories', 1000)
            self.max_pages = res_json.get('max_pages', 1000)
            self.page_threads = res_json.get('page_threads', 1)

            logger.info(f"Конфигурация загружена из {config_path}")

        except Exception as e:
            logger.error(f"Ошибка при загрузке конфигурации: {e}")
            raise

    def get_products_from_category(self, categ_link, parse_categories):
        """
        Функция для получения продуктов из заданной категории. Праметр parse_categories отвечает за то, парсим мы категории (и соот-но нужно ли находить подкатегории), или передаётся конечная страница категории, на которой просто нужно взять все продукты (как, например, происходит при заданной ТТ)
        
        Args:
            categ_link (str): Ссылка которую нужно распарсить
            parse_categories (bool): Нужно ли парсить категори (также необходимо, если есьт подкатегории)

        Returns:
            List: Список продуктов, полученных по заданной категориии (и всем страницам)
        """
        products_list = self.parsing_processor.process_category_parallel(
            categ_link=categ_link,
            parse_categories=parse_categories,
            page_threads=self.page_threads,
            max_pages=self.max_pages)

        return products_list

    def save_products_csv(self, products_list, cat_name):
        added_count = self.db_manager.create_products(products_list, cat_name)
        logger.info(f"Добавлено продуктов: {added_count}")

    def Parse(self):
        logger.info("Начало парсинга")
        base_url, cat_page_url = self.browser_emulator.start_emulation()
        if not (base_url and cat_page_url):
            logger.critical(
                "Не удалось определить город и ТТ для парсинга, завершаю работу"
            )
            return

        self.base_url, self.cat_page_url = base_url, cat_page_url
        self.parsing_processor = ParsingProcessor(self.base_url,
                                                  self.cat_page_url, logger,
                                                  self.config_path)
        if self.parse_categpries:
            categories_links = self.parsing_processor.get_catalogue_categories(
            )
            logger.info(f"Найдены категории: {categories_links}")

            categories_items = list(categories_links.items())
            selected_categories = categories_items[
                1:min(self.max_categories, len(categories_items)) +
                1]  # Пропускаем секцию с акционными товарами и идём до кол-ва категорий указанных в конфиге. Если оно будет больше, то мы просто будем идти по всем найденным дабы не было ошибки
            for category_name, categ_link in selected_categories:
                products_list = self.get_products_from_category(
                    categ_link, self.parse_categpries)
                self.save_products_csv(products_list,
                                       f"От Winestyle | {category_name}")

        else:
            products_list = self.get_products_from_category(
                self.cat_page_url, self.parse_categpries)
            self.save_products_csv(
                products_list, f"От Winestyle | Из ТТ {self.address}| Все")


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
