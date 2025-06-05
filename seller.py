import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получает список товаров из маркетплейса OZON.

    Функция делает POST запрос к API OZON, для получения списка товаров.
    Список начинается с указанного last_id товара.
    За один раз скачивает не более 1000 товаров.

    Args:
        last_id (str): Параметр обозначает последний товар.
        client_id (str): Ваш индивидуальный id с OZON.
        seller_token (str): Ваш индивидуальный токен с OZON.

    Returns:
        dict: Словарь полученный с OZON API.

    Raises:
        requests.HTTPError: Если в запросе произошла ошибка.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина OZON.

    Функция вызывает get_product_list и получает словарь
    с списком товаров из OZON по 1000 шт за раз.
    Затем извлекает артикулы товаров из ранее полученного словаря.
    По итогу создает список артикулов товаров с OZON.

    Args:
        client_id (str): Ваш индивидуальный id с OZON
        seller_token (str): Ваш индивидуальный токен с OZON.

    Returns:
        list: Список артикулов товаров.

    Raises:
        requests.HTTPError: Если в запросе к API произошла ошибка.
        KeyError: Если в словаре отсутствуют ожидаемые ключи.
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров на OZON.

    Функция с помощью POST запроса к OZON API обновляет цены
    в полученном списке товаров и возвращает словарь списков.

    Args:
        prices: Список с ценами на товар.
        client_id (str): Ваш индивидуальный id с OZON.
        seller_token (str): Ваш индивидуальный токен с OZON.

    Returns:
        dict: Словарь с обновленным списком цен товаров.

    Raises:
        requests.HTTPError: Если в запросе к API произошла ошибка.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки товаров на OZON.

    Функция с помощью POST запроса к API OZON обновляет информацию
    о количестве товара.

    Args:
        stocks: Список с информацией об остатках товара.
        client_id (str): Ваш индивидуальный id с OZON.
        seller_token (str): Ваш индивидуальный токен с OZON.

    Returns:
        dict: Словарь с обновленным списком остатков товаров.

    Raises:
        requests.HTTPError: Если в запросе к API произошла ошибка.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачивает файл ostatki с сайта timeworld.ru.

    Функция с помощью GET запроса к API timeworld.ru
    скачивает информацию об остатках товара архив ostatki.zip.
    Извлекает из архива exel файл ostatki.xls и берёт из него данные
    создавая список словарей с информацией об остатках товара.
    После удаляет уже не нужный ostatki.xls.

    Returns:
        list[dict]: Список словарей с информацией об остатках товара.

    Raises:
        requests.HTTPError: Если в запросе к API произошла ошибка.
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создаёт список остатков товара.

    Функция обрабатывает данные обоих списков и сравнивает их между собой
    по артикулам создавая новый список словарей.
    Изменяет количество товара опираясь на данные из списка watch_remnants.
    Если в списках нет совпадений то такому товару присваивается остаток - 0.

    Args:
        watch_remnants list[dict]: Список словарей
        с информацией об остатках товара с timeworld.ru.
        offer_ids list: Список артикулов товаров с OZON.

    Returns:
        list[dict]: Общий список словарей с информацией об остатках товаров
        с timeworld.ru и OZON.

    Raises:
        ValueError: Если количество не удалось преобразовать в число.
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создаёт новый список с актуальными ценами на товары.

    Функция перебирает товары в из списка watch_remnants
    и сравнивает по id с товарами в списке offer_ids.
    Если id совпадают, то товар отправляется в новый список словарей
    с обновленной ценой взятой из первого списка.

    Args:
        watch_remnants list[dict]: Список словарей
        с информацией об остатках товара с timeworld.ru.
        offer_ids list: Список артикулов товаров с OZON.

    Returns:
        list[dict]: Список словарей с актуальной ценой на товары.

    Raises:
        KeyError: Если в словаре отсутствуют ожидаемые ключи.
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовывает цену в целое число.

    Args:
        price: Цена товара.

    Returns:
        Возвращает целое число в формате строки без посторонних символов.

    Raises:
        AttributeError: Если будем передавать не строку

    Exemples:
        >>>a = "5'990.00 руб"
        >>>print(price_conversion(a)
        '5990'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список lst на списки по n элементов.

    Args:
        lst: Список остатков товара.
        n: Количество элементов.

    Yields:
        int: По n количеству будет разделен список.

    Raises:
        AttributeError: Если будем передавать не строку

    Exemples:
        >>>a = [1, 2, 3, 4, 5]
        >>>b = 2
        >>>print(list(divide(a, b)))
        [[1, 2], [3, 4], [5]]
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Обновляет цены товаров на OZON."""
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Обновляет остатки товаров на OZON."""
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """
    Основная функция для обновления остатков и цен товаров в OZON.

    Функция получает переменные окружения `SELLER_TOKEN` и `CLIENT_ID`.
    Загружает артикулы товаров из OZON, а также остатки и цены из timeworld.ru.
    Формирует и отправляет обновлённые остатки и цены через API OZON.

    Raises:
        requests.exceptions.ReadTimeout: Превышено время ожидания сервера.
        requests.exceptions.ConnectionError: Ошибка соединения.
        Exception: ERROR_2.
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
