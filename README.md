# Парсер для сайта WineStyle

## 🔍 Общее описание

Парсер предназначен для автоматизированного сбора данных с сайта WineStyle с использованием многопоточной обработки. Все данные записываются в products.csv файл

## ✨ Ключевые особенности

-**Многопоточность**: Параллельный сбор данных с использованием `concurrent.futures`

-**Гибкая конфигурация**: Настройка через JSON-файл

-**Подробное логирование**: Запись всех событий и ошибок

-**Модульная архитектура**: Разделение на компоненты парсинга и эмулятора действй selenium.

## 🚀 Запуск

```bash
python main.py
```

## 📋 Требования

- Python 3.10+
- Зависимости указаны в requirements.txt

## 🛠 Компоненты системы

### Основные классы

1.**Parser**:

- Центральный класс управления парсингом
- Инициализация с менеджером базы данных и конфигурацией
- Координация процессов парсинга

2.**ParsingProcessor**:

- Извлечение категорий и сохранение как названия категории, так и соответствующей к ней ссылке
- Многопоточная обработка каждой страницы, существующей в категории
- Многопоточная обработка продуктов на странице

3.**Emulator**:

- Эмулятор действий пользователя для корректного выбора города и ТТ из конфигурационного файла.
- Отдельный логгер, полезно при возникновении ошибок

## 🔧 Workflow парсинга

1. Загрузка конфигурации
2. Запуск эмулятора для определения параметров города и ТТ
3. Если парсинг категорий включён - получение списка категорий. Иначе следующий шаг
4. Двухуровневый иногопоточный сбор продуктов
5. Загрузка продуктов их информации
6. Сохранение в CSV

## 📦 Конфигурация

Файл `config.json` позволяет настроить:

- Город парсинга
- Адрес ТТ для парсинга
- Прокси
- Парсинг категорий - важный параметр, если необходимо получить продукты из конкретной ТТ, то нужно поставить false. При значении true парсер будет собирать из указанного в конфиге города, но не ТТ. Зато он будет проходиться по всем найденным категориям в этом городе
- Местоположение Chrome.exe - также важный параметр, без него попросту не запустится эмулятор для выбора города и ТТ. В конфиге указано обычное расположение - если у вас другое, необходимо изменить.
- Настраиваемое количество потоков для разных задач - парсинга категорий, страниц и продуктов. По надобности для каждой задачи можно указать своё кол-во потоков
- Backoff factor, позволяющий динамично изменять время для запроса (позволяет серверу сайта не "упасть", а также лучше имитирует время человеских запросов, что уменьшает вероятность блокировки)
- Максимальное число категорий для обработки (на случай, если нам нужно ограничить работу парсера в целях теста/обхода блокировок)
- Максимальное число страниц дя обработки (по тем же самым причинам)

## 🚨 Обработка ошибок

- Подробное логирование всех исключительных ситуаций
- Отдельная запись действий парсера в файл WineStyleParser.log, а эмулятора - в файле BrowserEmulator.log

## 💾 Хранение данных

Используется `DBManager` для сохранения продуктов в CSV-файл `products.csv`

## ⚠️ Примечания

- Количество потоков влияет на нагрузку на сервер
- В папке tested можно найти данные и логи, которые были получены во время тестов
