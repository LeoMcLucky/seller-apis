import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получает список товаров из Yandex market.

    Функция делает GET запрос к API Yandex market,
    для получения списка товаров.
    Список начинается с указанной страницы page.
    За один раз скачивает не более 200 товаров.

    Args:
        page (str): Токен обозначающий номер страници.
        campaign_id (str): Ваш индивидуальный id компании для Yandex market.
        access_token (str): Ваш индивидуальный токен для Yandex market.

    Returns:
        dict: Словарь полученный из Yandex market API
        с списком товаров и данными о пагинации.

    Raises:
        requests.HTTPError: Если в запросе произошла ошибка.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить остатки товаров на Yandex market.

    Функция с помощью PUT метода к API Yandex market обновляет информацию
    о количестве товара. Нагрузкой По 2000 шт. товара за раз.

    Args:
        stocks list[dict]: Список словарей с информацией об остатках товара.
        campaign_id (str): Ваш индивидуальный id компании для Yandex market.
        access_token (str): Ваш индивидуальный токен для Yandex market.

    Returns:
        dict: Словарь с обновленным списком остатков товаров.

    Raises:
        requests.HTTPError: Если в запросе к API произошла ошибка.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновить цены товаров на Yandex market.

    Функция с помощью POST метода к Yandex market API обновляет цены
    в полученном списке товаров и возвращает словарь списков.

    Args:
        prices list[dict]: Список с ценами на товар.
        campaign_id (str): Ваш индивидуальный id компании для Yandex market.
        access_token (str): Ваш индивидуальный токен для Yandex market.

    Returns:
        dict: Словарь с информацией о статусе обновления цен.

    Raises:
        requests.HTTPError: Если в запросе к API произошла ошибка.
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров  Yandex market.

    Функция вызывает get_product_list получает словарь
    со списком товаров из Yandex Маркет по 200 шт за раз.
    Вызывает функцию до тех пор пока не закончатся страницы с товарами.
    Далее созаёт список артикулов Sku товаров с Yandex market.

    Args:
        campaign_id (str): Ваш индивидуальный id компании с Yandex market.
        market_token (str): Ваш индивидуальный токен с Yandex market.

    Returns:
        list: Список артикулов товаров SKU(внутренний ID карточки товара) Yandex market.

    Raises:
        requests.HTTPError: Если в запросе к API произошла ошибка.
        KeyError: Если в словаре отсутствуют ожидаемые ключи.
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создаёт список остатков товара.

    Функция обрабатывает данные обоих списков и сравнивает их между собой
    по артикулам создавая новый список словарей.
    Изменяет количество товара опираясь на данные из списка watch_remnants:
    Товаров >10 = 100шт.
    Товар 1 = 0.
    Либо = реальное количество товара.
    Оставшиеся товары из списка Yandex market у которых нет совпадений добавляются с остатком 0.

    Args:
        watch_remnants list[dict]: Список словарей с информацией об остатках товара с timeworld.ru.
        offer_ids list: Список артикулов товаров SKU(внутренний ID карточки товара) Yandex market.
        warehouse_id (str): ID склада.

    Returns:
        list[dict]: Общий список словарей с информацией об остатках товаров
        с timeworld.ru и Yandex market.

    Raises:
        ValueError: Если количество не удалось преобразовать в число.
        KeyError: Если в словаре отсутствуют ожидаемые ключи.
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(
        microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создаёт новый список с актуальными ценами на товары.

    Функция перебирает товары в из списка watch_remnants
    и сравнивает по id с товарами в списке offer_ids.
    Если id совпадают, то товар отправляется в новый список словарей
    с обновленной ценой взятой из первого списка.

    Args:
        watch_remnants list[dict]: Список словарей с информацией об остатках товара с timeworld.ru.
        offer_ids list: Список артикулов товаров SKU(внутренний ID карточки товара) Yandex market.

    Returns:
        list[dict]: Список словарей с актуальной ценой на товары.

    Raises:
        KeyError: Если в словаре отсутствуют ожидаемые ключи.
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Обновляет цены товаров на Yandex market.

    Args:
        watch_remnants list[dict]: Список словарей с информацией об остатках товара с timeworld.ru.
        campaign_id (str): Ваш индивидуальный id компании для Yandex market.
        market_token (str): Ваш индивидуальный токен с Yandex market.

    Returns:
        list[dict]: Список словарей с актуальной ценой на товары.

    Raises:
        requests.HTTPError: Если в запросе к API произошла ошибка.
        KeyError: Если в словаре отсутствуют ожидаемые ключи.
        requests.HTTPError: Если в запросе к API произошла ошибка.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Обновляет остатки товаров на Yandex market.

    Args:
        watch_remnants list[dict]: Список словарей с информацией об остатках товара с timeworld.ru.
        campaign_id (str): Ваш индивидуальный id компании для Yandex market.
        market_token (str): Ваш индивидуальный токен с Yandex market.
        warehouse_id (str): ID склада.


    Returns:
        list[dict]: Общий список словарей с информацией об остатках товаров
        с timeworld.ru и Yandex market.
        not_empty list: Cписок товаров с остатком > 0

    Raises:
        requests.HTTPError: Если в запросе к API произошла ошибка.
        KeyError: Если в словаре отсутствуют ожидаемые ключи.
        ValueError: Если количество не удалось преобразовать в число.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """
    Основная функция для обновления остатков и цен товаров в Yandex market.

    Функция получает переменные окружения:
    MARKET_TOKEN,
    FBS_ID,
    WAREHOUSE_FBS_ID
    DBS_ID,
    WAREHOUSE_DBS_ID.
    Загружает артикулы товаров из Yandex market по двум моделям логистики FBS и DBS.
    Далее остатки и цены товаров из timeworld.ru.
    Затем формирует и отправляет обновлённые остатки и цены через API Yandex market.

    Raises:
        requests.exceptions.ReadTimeout: Превышено время ожидания сервера.
        requests.exceptions.ConnectionError: Ошибка соединения.
        Exception: ERROR_2.
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
