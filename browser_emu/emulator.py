import os
import logging
from logging import Logger
import traceback
import json
from time import sleep
from difflib import SequenceMatcher

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException


class Emulator:

    def __init__(self, logger, config_path):
        self.base_url = "https://winestyle.ru"
        self.cet_page_url = "https://winestyle.ru"
        self.city = "Москва"
        self.address = ""
        self.logger: Logger = logger
        self.driver = None

        self._load_config(config_path)
        self._initiallize_driver()

        self.logger.info(
            f"Запущена эмуляция бразуера.\nЦелевой город: {self.city}\nЦелевая ТТ: {self.address}"
        )

    def _load_config(self, config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as config_file:
                res_json = json.load(config_file)

                self.city = res_json.get('city', "Москва")
                self.address = res_json.get('address', "")

        except Exception as e:
            self.logger.error(
                f"Ошибка при загрузке конфигурации ParsingProcessor: {e}")
            raise

    def _initiallize_driver(self):
        try:
            chromedriver_path = os.path.join(os.path.dirname(__file__),
                                             'chromedriver.exe')
            options = webdriver.ChromeOptions()
            options.add_argument('--start-maximized')
            options.add_argument('--disable-extensions')
            options.add_argument('--no-sandbox')

            options.binary_location = r"C:\\Program Files\\Google\\Chrome Beta\\Application\\chrome.exe"

            service = Service(executable_path=chromedriver_path)
            self.driver = webdriver.Chrome(service=service, options=options)

            return True
        except Exception as e:
            self.logger.critical(
                f"Произошла ошибка при инициализации селениума! {e}")
            return False

    def choose_city(self):
        try:
            self.driver.get(self.base_url)
            try:
                sleep(3)

                header_element = self.driver.find_element(
                    By.CLASS_NAME, "header-bar__item")

                self.driver.execute_script(
                    "arguments[0].scrollIntoView(true);", header_element)
                sleep(1)

                actions = ActionChains(self.driver)
                actions.move_to_element(header_element).click().perform()

                self.logger.info("Успешный клик по элементу выбора города")
                sleep(10)

                city_list = self.driver.find_element(
                    By.XPATH,
                    "/html/body/div[1]/div/div[6]/div[2]/div/div/div[3]/ul[2]")

                city_items = city_list.find_elements(By.TAG_NAME, "li")

                # Поиск и клик по городу
                for item in city_items:
                    full_text = item.text.strip()
                    ratio = SequenceMatcher(None, self.city, full_text).ratio()
                    # self.logger.info(f"Город Ratio: {ratio}")

                    if ratio >= 0.8:
                        item.click()
                        self.logger.info(f"Выбран город: {full_text}")
                        break
                    # else:
                    #     self.logger.info(f"Город {full_text} пропущен")

                hashtag = self.driver.current_url.find("#")
                if hashtag > -1:
                    self.base_url = self.driver.current_url[:hashtag]
                else:
                    self.base_url = self.driver.current_url

                return True

            except NoSuchElementException as e:
                self.logger.error(f"Элемент не найден: {e}")
                return False

        except Exception as e:
            self.logger.error(f"Ошибка в start_emulation: {e}")
            self._close_driver()
            raise

    def choose_TT(self):
        try:
            self.driver.get(self.base_url)
            try:
                sleep(3)

                shop_button = self.driver.find_element(
                    By.XPATH,
                    "/html/body/div[1]/div/div[1]/div/div/div/div[2]/div[2]/span"
                )
                if not shop_button:
                    self.logger.critical(
                        "Не удалось найти кнопку с выбором магазина!")
                    self._close_driver()
                    return False

                shop_button.click()
                self.logger.info("Клик по кнопке магазинов успешный")

                sleep(5)

                TT_list = self.driver.find_element(
                    By.XPATH,
                    "/html/body/div[1]/div/div[6]/div[2]/div/div/div[1]/div/div[2]/div/div"
                )

                TT_items = TT_list.find_elements(By.CLASS_NAME,
                                                 "m-map-shops-item__text")

                # Поиск и клик по адресу ТТ
                self.logger.info(f"Нужный адрес: {self.address}")
                for item in TT_items:
                    full_text = item.text.strip()
                    ratio = SequenceMatcher(None, self.address,
                                            full_text).ratio()
                    # self.logger.info(f"Ratio {ratio}")
                    if ratio >= 0.8:
                        item.click()
                        self.logger.info(f"Выбрана ТТ: {full_text}")
                        break
                    else:
                        self.logger.info(f"Адрес {full_text} пропущен")

                sleep(3)
                mag_button = self.driver.find_element(
                    By.XPATH,
                    "/html/body/div[1]/div/div[6]/div[2]/div/div/div[1]/div/div[2]/div/div/div[1]/div[2]/button"
                )
                mag_button.click()
                sleep(10)
                assortiment_button = self.driver.find_element(
                    By.XPATH,
                    "/html/body/div[1]/div/div[4]/div/div[1]/div[2]/div[2]/div/div[4]/button[2]"
                )
                assortiment_button.click()
                sleep(5)

                self.cet_page_url = self.driver.current_url

                return True

            except NoSuchElementException as e:
                self.logger.error(f"Элемент не найден: {e}")
                raise

        except Exception as e:
            self.logger.error(f"Ошибка в start_emulation: {e}")
            self._close_driver()
            raise

    def start_emulation(self):
        try:
            chosen_city = self.choose_city()
            if not chosen_city:
                self.logger.critical("Не удалось найти нужный город!")
                self._close_driver()
                return False, False

            self.logger.info(
                f"Город успешно определён, базовая ссылка: {self.base_url}")

            chosen_TT = self.choose_TT()
            if not chosen_TT:
                self.logger.critical("Не удалось определить ТТ!")
                self._close_driver()
                return False, False

            self.logger.info(
                f"ТТ успешно определена, страница с её товарами: {self.cet_page_url}"
            )
            self._close_driver()

            return self.base_url, self.cet_page_url

        except Exception as e:
            self.logger.error(f"Ошибка в start_emulation: {e}")
            self._close_driver()
            raise

    def keep_browser_open(self):
        input("Нажмите Enter для закрытия браузера...")
        self._close_driver()

    def _close_driver(self):
        if self.driver:
            self.driver.quit()

    def __del__(self):
        self._close_driver()


if __name__ == "__main__":
    try:
        logger = logging.getLogger('Emulator')
        handler = logging.FileHandler('Emulator.log', encoding="utf-8")
        handler.setFormatter(
            logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        emu = Emulator(logger, "../config.json")
        emu.start_emulation()

    except Exception as e:
        logger.critical(f"Кртитическая ошибка при работе эмулятора {e}")
        logger.error(traceback.format_exc())
