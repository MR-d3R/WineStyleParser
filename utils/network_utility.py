import json
import requests
from time import sleep
from random import uniform


class NetworkConnector:

    def __init__(self, logger, config_path):
        self.logger = logger

        self.proxy = ""
        self.max_retries = 3
        self.backoff_factor = 0.3

        self._load_config(config_path)
        self.headers = {
            'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = requests.Session()
        self.session.proxies = {
            'http': f'http://{self.proxy}',
            'https': f'http://{self.proxy}',
        }

        # Настройка адаптера для повторных попыток
        retry_strategy = requests.adapters.Retry(
            total=self.max_retries,  # Максимальное количество попыток
            backoff_factor=self.
            backoff_factor,  # Экспоненциальная задержка между попытками
            status_forcelist=[500, 502, 503,
                              504],  # Коды ошибок для повторных попыток
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

    def _load_config(self, config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as config_file:
                res_json = json.load(config_file)

                self.proxy = res_json.get("proxy", "")
                self.backoff_factor = res_json.get('backoff_factor', 0.3)
                self.max_retries = res_json.get('max_retries', 3)

        except Exception as e:
            self.logger.error(
                f"Ошибка при загрузке конфигурации network_utility: {e}")
            raise

    def exponential_backoff(self, attempt: int, max_time: float = 120.0):
        """
            Экспоненциальная задержка с джиттером для предотвращения конфликтов.
            
            :param attempt: Номер текущей попытки
            :param max_time: Максимальное время задержки
            """
        backoff = min(max_time, (2**attempt) + uniform(0, 1))
        self.logger.info(
            f"Attempt {attempt}: Backoff for {backoff:.2f} seconds")
        sleep(backoff)

    def safe_request(self, url, method='get', max_attempts=3, **kwargs):
        """
            Безопасный метод выполнения HTTP-запросов с обработкой ошибок.
            
            :param url: URL для запроса
            :param method: HTTP метод
            :param max_attempts: Максимальное количество попыток
            :param kwargs: Дополнительные аргументы для requests
            :return: Результат запроса или None
            """
        for attempt in range(1, max_attempts + 1):
            try:
                if method.lower() == 'get':
                    response = self.session.get(url,
                                                headers=self.headers,
                                                **kwargs)
                elif method.lower() == 'post':
                    response = self.session.post(url,
                                                 headers=self.headers,
                                                 **kwargs)

                else:
                    raise ValueError(f"Неподдерживаемый метод: {method}")

                response.raise_for_status()
                return response

            except requests.RequestException as e:
                self.logger.warning(f"Ошибка запроса {url}: {e}")
                if attempt >= max_attempts:
                    self.logger.error(
                        f"Превышено максимальное количество попыток для {url}")
                    return None
                else:
                    self.logger.info(f"Повторная попытка для {url}")

                self.exponential_backoff(attempt)
