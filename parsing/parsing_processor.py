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

        self.address = ""
        self.max_pages = 1000
        self._load_config(config_path)

        self.network_connector = NetworkConnector(logger, config_path)

    def _load_config(self, config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as config_file:
                res_json = json.load(config_file)

                self.address = res_json.get('address', "")
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
                         is_last_page=False,
                         product_threads=4):
        """
        Обрабатывает категорию товаров.
        
        Args:
            categ_link: ссылка на категорию
            is_first_page: флаг, указывающий является ли это первой страницей категории
            is_last_page: флаг, указывающий является ли это последней известной страницей
            product_threads: количество потоков для обработки продуктов
        """
        self.logger.info(f"Обработка категории: {categ_link}")

        # Получаем ссылку на все продукты только для первой страницы
        all_prod_link = categ_link
        if is_first_page:
            all_prod_link = self.get_all_products_in_category_link(categ_link)
            if not all_prod_link:
                self.logger.error(
                    "Ошибка при получении контейнера со всеми продуктами!")
                return [], []

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

            # Параллельная обработка продуктов
            with ThreadPoolExecutor(max_workers=product_threads) as executor:
                # Запускаем обработку продуктов параллельно
                future_to_product = {}
                for product in all_products_list:
                    future = executor.submit(self.process_product, product)
                    future_to_product[future] = product
                # Собираем результаты обработки продуктов
                for future in as_completed(future_to_product):
                    product = future_to_product[future]
                    try:
                        prod_res = future.result()
                        if prod_res:
                            result_products_list.append(prod_res)
                    except Exception as e:
                        self.logger.error(
                            f"Ошибка при обработке продукта: {e}")

        # Получаем ссылки на другие страницы если это первая страница или последняя известная
        new_pagination_links = []
        if is_first_page or is_last_page:
            pagination_pages = soup.find('div', class_='ws-pagination__pages')
            if pagination_pages:
                pag_hrefs = pagination_pages.find_all("a")
                for pag in pag_hrefs:
                    num = pag.get_text()
                    link = all_prod_link.split('?')[0] + f"?page={num}"
                    new_pagination_links.append(link)

        return result_products_list, new_pagination_links

    def process_category_parallel(self,
                                  categ_link,
                                  page_threads=4,
                                  product_threads=4,
                                  max_pages=10):
        """
        Параллельная обработка всех страниц категории и продуктов.
    
        Args:
            categ_link: ссылка на категорию
            page_threads: количество потоков для обработки страниц
            product_threads: количество потоков для обработки продуктов на каждой странице
            max_pages: максимальное количество страниц для обработки
        """
        # Обрабатываем первую страницу и получаем начальные ссылки на пагинацию
        first_page_results, pagination_links = self.process_category(
            categ_link, is_first_page=True, product_threads=product_threads)
        all_results = first_page_results
        processed_links = {categ_link}

        # Счетчик обработанных страниц
        page_count = 1  # Первая страница уже обработана

        while pagination_links and page_count < max_pages:
            new_links = [
                link for link in pagination_links
                if link not in processed_links
            ]
            if not new_links:
                break

            last_link = new_links[-1]
            with ThreadPoolExecutor(max_workers=page_threads) as executor:
                future_to_url = {}
                for link in new_links:
                    is_last = (link == last_link)
                    future = executor.submit(self.process_category, link,
                                             False, is_last, product_threads)
                    future_to_url[future] = link

                new_pagination_links = set()
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        page_results, page_pagination = future.result()
                        all_results.extend(page_results)
                        if url == last_link:
                            new_pagination_links.update(page_pagination)
                        processed_links.add(url)
                        page_count += 1  # Увеличиваем счетчик страниц

                        # Прерываем, если достигли максимального числа страниц
                        if page_count >= max_pages:
                            break
                    except Exception as e:
                        self.logger.error(
                            f"Ошибка при обработке страницы {url}: {e}")

                    # Проверяем счетчик страниц еще раз
                    if page_count >= max_pages:
                        break

                # Обновляем ссылки пагинации
                pagination_links = list(new_pagination_links - processed_links)

                # Прерываем цикл, если достигли максимума страниц
                if page_count >= max_pages:
                    break

        # Логируем общее количество обработанных страниц
        self.logger.info(f"Обработано страниц: {page_count}")

        return all_results

    def get_product_link(self, product):
        name_container = product.find("div", "m-catalog-item__info")
        product_link = name_container.find("a")

        full_url = False
        if product_link and 'href' in product_link.attrs:
            full_url = urljoin(self.base_url, product_link["href"])

        return full_url

    def get_product_name(self, product_page):
        try:
            name_container = product_page.find(
                'div', class_='o-productpage-info__title')
            name_h = name_container.find("h1", "heading heading--3xl")

            product_name = name_h.get_text()
            self.logger.info(f"Название продукта: {product_name}")

            return product_name
        except Exception as e:
            self.logger.warning(f"Не удаётся получить имя продукта!! {e}")
            return False

    def get_product_article(self, product_page):
        try:
            article_container = product_page.find(
                'div', class_='o-productpage-info__controls')
            article_spans = article_container.find_all("span")

            self.logger.info(f"Артикли {article_spans}")

            article = article_spans[2]
            for span in article_spans:
                span_text = span.get_text(strip=True)
                if "Артикул:" in span_text:
                    # Извлекаем текст после "Артикул:"
                    article = span_text.split("Артикул:")[-1].strip()
                    break

            self.logger.info(f"Артикул продукта: {article}")

            return article

        except Exception as e:
            self.logger.warning(f"Не удаётся получить артикли продукта!! {e}")
            return False

    def get_product_price(self, product_page):
        try:
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
            self.logger.info(f"Цены продукта: {prices}")

            return prices
        except Exception as e:
            self.logger.warning(f"Не удаётся получить цены продукта!! {e}")
            return False

    def process_product(self, product):
        product_link = self.get_product_link(product)

        self.logger.info(f"Ссылка на продукт: {product_link}")

        if not product_link:
            self.logger.error(
                "Произошла ошибка при получении ссылки на продукт!")
            return False

        response = self.network_connector.safe_request(product_link)
        soup = BeautifulSoup(response.text, 'html.parser')

        res_product = Product()
        pr_name = self.get_product_name(soup)
        pr_article = self.get_product_article(soup)
        pr_prices = self.get_product_price(soup)
        if pr_name and pr_article and pr_prices:
            res_product.name = pr_name
            res_product.article = pr_article
            res_product.prices = pr_prices
        else:
            self.logger.warning("Не удалось получить все данные продукта!")
            return False

        self.logger.info(
            f"\n{res_product.name} \n {res_product.article}\n{res_product.prices}"
        )

        res_product.datetime = datetime.now()
        res_product.shop = self.address
        # TODO добавить название категории откуда получили

        return res_product
