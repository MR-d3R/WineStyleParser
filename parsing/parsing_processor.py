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

    def __init__(self, base_url, cat_page_url, logger, config_path):
        self.base_url = base_url
        self.cat_page_url = cat_page_url
        self.logger: Logger = logger

        self.address = ""
        self.max_pages = 1000
        self.max_threads = 1
        self.page_threads = 1
        self.product_threads = 1
        self._load_config(config_path)

        self.network_connector = NetworkConnector(logger, config_path)

    def _load_config(self, config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as config_file:
                res_json = json.load(config_file)

                self.address = res_json.get('address', "")
                self.max_pages = res_json.get('max_pages', 1000)
                self.max_threads = res_json.get('threads', 1)
                self.page_threads = res_json.get('page_threads', 1)
                self.product_threads = res_json.get('product_threads', 1)

        except Exception as e:
            self.logger.error(
                f"Ошибка при загрузке конфигурации ParsingProcessor: {e}")
            raise

    def get_catalogue_categories(self):
        categories_links = {}
        try:
            response = self.network_connector.safe_request(self.base_url)

            soup = BeautifulSoup(response.text, 'html.parser')

            categories_container = soup.find('div', class_='carousel__list')

            abstract_categories = categories_container.find_all(
                "div", class_="header-categories__item")

            for category in abstract_categories:
                category_link = category.find("a")
                if category_link and 'href' in category_link.attrs:
                    categ_text = category_link.get_text()
                    full_url = urljoin(self.base_url, category_link["href"])
                    categories_links[categ_text] = full_url

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

        all_prod_link = categ_link
        if is_first_page:
            all_prod_link = self.get_all_products_in_category_link(categ_link)
            if not all_prod_link:
                self.logger.error(
                    "Ошибка при получении контейнера со всеми продуктами!")
                return [], []

        response = self.network_connector.safe_request(all_prod_link,
                                                       method="get")
        soup = BeautifulSoup(response.text, 'html.parser')

        # Обработка продуктов на текущей странице
        result_products_list = []
        all_products_container = soup.find('div', class_='ws-products__list')
        if all_products_container:
            all_products_list = all_products_container.find_all(
                "div", class_="m-catalog-item--grid")
            if not all_products_list:
                all_products_list = all_products_container.find_all(
                    "div", class_="m-catalog-item--list")

            with ThreadPoolExecutor(
                    max_workers=self.product_threads) as executor:
                future_to_product = {}
                for product in all_products_list:
                    future = executor.submit(self.process_product, product)
                    future_to_product[future] = product

                for future in as_completed(future_to_product):
                    product = future_to_product[future]
                    try:
                        prod_res = future.result()
                        if prod_res:
                            for result in prod_res:
                                if result:
                                    result_products_list.append(result)
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
                                  parse_categories: bool,
                                  page_threads=4,
                                  max_pages=10):
        """
        Параллельная обработка всех страниц категории и продуктов.
    
        Args:
            categ_link: ссылка на категорию
            parse_categories (bool): Нужно ли парсить категори (также необходимо, если есть подкатегории)
            page_threads: количество потоков для обработки страниц
            max_pages: максимальное количество страниц для обработки
        """
        need_to_get_pagination = not parse_categories
        first_page_results, pagination_links = self.process_category(
            categ_link,
            is_first_page=parse_categories,
            is_last_page=need_to_get_pagination)
        all_results = first_page_results
        processed_links = {categ_link}

        page_count = 1

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
                    is_really_first_page = False
                    is_last = (link == last_link)
                    future = executor.submit(self.process_category, link,
                                             is_really_first_page, is_last)
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
                        page_count += 1

                        # Прерываем, если достигли максимального числа страниц
                        if page_count >= max_pages:
                            break
                    except Exception as e:
                        self.logger.error(
                            f"Ошибка при обработке страницы {url}: {e}")
                    if page_count >= max_pages:
                        break

                # Обновление ссылок пагинации
                pagination_links = list(new_pagination_links - processed_links)

                if page_count >= max_pages:
                    break

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

    def get_product_variations(self, product_link: str, product_page):
        try:
            variatons_container = product_page.find(
                "div", class_="o-productpage-info__volume")
            if not variatons_container:
                self.logger.error(
                    f"Кажется у продукта нет вариаций {product_link}")
                return False

            variatons_href = variatons_container.find_all("a")

            link = self.base_url + "/products"
            var_links = []
            for var in variatons_href:
                if var and 'href' in var.attrs:
                    full_url = urljoin(link, var["href"])
                    var_links.append(full_url)

            self.logger.info(f"Массив с вариациями: {var_links}")
            return var_links

        except Exception as e:
            self.logger.warning(
                f"Не удаётся получить ссылки на вариации продукта!! {e}")
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

        processed_products = []
        var_links = self.get_product_variations(product_link, soup)
        if var_links:
            for link in var_links:
                processed_product = self.process_exact_product(link)
                processed_products.append(processed_product)
        else:
            processed_product = self.process_exact_product(product_link)
            processed_products.append(processed_product)

        return processed_products

    def check_product_exists(self, link, product_page):
        exists_span = product_page.find("span", "m-productpage-price__status")
        exists_str: str = exists_span.get_text()
        exists_str = exists_str.lower()
        if "нет" in exists_str:
            self.logger.error(f"Продукта нет в наличии {link}")
            return False

        return True

    def process_exact_product(self, link):
        response = self.network_connector.safe_request(link)
        soup = BeautifulSoup(response.text, 'html.parser')

        prod_exists = self.check_product_exists(link, soup)
        if not prod_exists:
            return False

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

        return res_product
