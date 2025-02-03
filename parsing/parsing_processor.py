import re
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from utils.network_utility import NetworkConnector
from logging import Logger
from typing import List
from datetime import datetime
from models import Product
from concurrent.futures import ThreadPoolExecutor, as_completed


class ParsingProcessor:

    def __init__(self, base_url, logger, config_path):
        self.base_url = base_url
        self.logger: Logger = logger

        self.max_pages = 1000
        self._load_config(config_path)

        self.network_connector = NetworkConnector(logger, config_path)

    def _load_config(self, config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as config_file:
                res_json = json.load(config_file)

                self.max_pages = res_json.get('max_pages', 1000)

        except Exception as e:
            self.logger.error(
                f"Ошибка при загрузке конфигурации ParsingProcessor: {e}")
            raise

    def get_catalogue_categories(self):
        categories_links = []
        try:
            response = self.network_connector.safe_request(self.base_url)

            soup = BeautifulSoup(response.text, 'html.parser')

            categories_container = soup.find('div', class_='carousel__list')

            abstract_categories = categories_container.find_all(
                "div", class_="header-categories__item")

            for category in abstract_categories:
                category_link = category.find("a")
                if category_link and 'href' in category_link.attrs:
                    full_url = urljoin(self.base_url, category_link["href"])
                    categories_links.append(full_url)

            return categories_links

        except requests.RequestException as e:
            self.logger.error(f"Ошибка при запросе категорий: {e}")
            return categories_links

    def get_all_products_in_category_link(self, categ_link):
        response = self.network_connector.safe_request(categ_link)
        soup = BeautifulSoup(response.text, 'html.parser')

        all_products_link = soup.find('a', class_='popular-category')

        full_url = False
        if all_products_link and 'href' in all_products_link.attrs:
            full_url = urljoin(categ_link, all_products_link["href"])

        return full_url

    def process_category(self,
                         categ_link,
                         is_first_page=True,
                         is_last_page=False):
        """
        Обрабатывает категорию товаров.
        
        Args:
            categ_link: ссылка на категорию
            is_first_page: флаг, указывающий является ли это первой страницей категории
            is_last_page: флаг, указывающий является ли это последней известной страницей
        """
        self.logger.info(f"Обработка категории: {categ_link}")

        # Получаем ссылку на все продукты только для первой страницы
        all_prod_link = categ_link
        if is_first_page:
            all_prod_link = self.get_all_products_in_category_link(categ_link)
            if not all_prod_link:
                self.logger.error(
                    "Ошибка при получении контейнера со всеми продуктами!")
                return []

        # Получаем и обрабатываем текущую страницу
        response = self.network_connector.safe_request(all_prod_link,
                                                       method="get")
        soup = BeautifulSoup(response.text, 'html.parser')

        # Обработка продуктов на текущей странице
        result_products_list = []
        all_products_container = soup.find('div', class_='ws-products__list')
        if all_products_container:
            all_products_list = all_products_container.find_all(
                "div", class_="m-catalog-item--grid")
            for product in all_products_list:
                prod_res = self.process_product(product)
                if prod_res:
                    result_products_list.append(prod_res)

        # Получаем ссылки на другие страницы если это первая страница или последняя известная
        new_pagination_links = []
        if is_first_page or is_last_page:
            pagination_pages = soup.find('div', class_='ws-pagination__pages')
            if pagination_pages:
                pag_hrefs = pagination_pages.find_all("a")
                for pag in pag_hrefs:
                    num = pag.get_text()
                    link = all_prod_link.split(
                        '?'
                    )[0] + f"?page={num}"  # Убираем существующие параметры page
                    new_pagination_links.append(link)

        return result_products_list, new_pagination_links

    def process_category_parallel(self, categ_link, num_threads=4):
        """
            Параллельная обработка всех страниц категории.
            
            Args:
                categ_link: ссылка на категорию
                num_threads: количество потоков для параллельной обработки
            """
        # Обрабатываем первую страницу и получаем начальные ссылки на пагинацию
        first_page_results, pagination_links = self.process_category(
            categ_link, is_first_page=True)
        all_results = first_page_results
        processed_links = {categ_link
                           }  # Множество для отслеживания обработанных ссылок

        while pagination_links:
            # Отфильтровываем уже обработанные ссылки
            new_links = [
                link for link in pagination_links
                if link not in processed_links
            ]
            if not new_links:
                break

            # Определяем последнюю страницу в текущем наборе
            last_link = new_links[-1]

            # Создаем пул потоков для обработки страниц
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                # Запускаем обработку страниц параллельно
                future_to_url = {}
                for link in new_links:
                    is_last = (link == last_link)
                    future = executor.submit(self.process_category, link,
                                             False, is_last)
                    future_to_url[future] = link

                # Собираем результаты и новые ссылки пагинации
                new_pagination_links = set()
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        page_results, page_pagination = future.result()
                        all_results.extend(page_results)
                        if url == last_link:  # Только для последней страницы сохраняем новые ссылки
                            new_pagination_links.update(page_pagination)
                        processed_links.add(url)
                    except Exception as e:
                        self.logger.error(
                            f"Ошибка при обработке страницы {url}: {e}")

                # Обновляем список ссылок для следующей итерации
                pagination_links = list(new_pagination_links - processed_links)

        return all_results

    def get_product_link(self, product):
        name_container = product.find("div", "m-catalog-item__info")
        product_link = name_container.find("a")

        full_url = False
        if product_link and 'href' in product_link.attrs:
            full_url = urljoin(self.base_url, product_link["href"])

        return full_url

    def get_product_name(self, product_page):
        name_container = product_page.find('div',
                                           class_='o-productpage-info__title')
        name_h = name_container.find("h1", "heading heading--3xl")

        product_name = name_h.get_text()

        return product_name

    def get_product_article(self, product_page):
        article_container = product_page.find(
            'div', class_='o-productpage-info__controls')
        article_span = article_container.find("span")

        article = article_span.get_text()

        return article

    def get_product_price(self, product_page):
        price_container = product_page.find('div',
                                            class_='m-productpage-price')
        prices_str = price_container.get_text()

        price_parts = [
            part.strip() for part in prices_str.split('₽') if part.strip()
        ]

        prices = []
        for part in price_parts:
            # Ищем числа в части строки
            match = re.search(r'\d+(?:\s+\d+)*', part)
            if match is None:
                # self.logger.error(f"Не найдены числа в строке: '{part}'")
                continue

            clean_price = re.sub(r'\s+', '', match.group())
            try:
                price = int(clean_price)
                if price > 100:
                    prices.append(price)

            except ValueError as e:
                self.logger.warning(
                    f"Не удалось преобразовать строку '{clean_price}' в число: {e}"
                )
                continue

        # Убираем дубликаты и сортируем цены
        prices = sorted(list(set(prices)))

        return prices

    def process_product(self, product):
        product_link = self.get_product_link(product)

        if not product_link:
            self.logger.error(
                "Произошла ошибка при получении ссылки на продукт!")
            return False
        response = self.network_connector.safe_request(product_link)
        soup = BeautifulSoup(response.text, 'html.parser')

        res_product = Product()
        res_product.name = self.get_product_name(soup)
        res_product.article = self.get_product_article(soup)
        res_product.prices = self.get_product_price(soup)
        self.logger.info(
            f"\n{res_product.name} \n {res_product.article}\n{res_product.prices}"
        )
        res_product.datetime = datetime.now()

        return res_product
