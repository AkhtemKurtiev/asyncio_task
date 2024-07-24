from contextlib import contextmanager
import datetime
import os
import re
import requests
import time

from xlrd import open_workbook

from constants import SKIP_WORDS, URL
from models.database import Session
from models.spimex_trading_results import Spimex_trading_results
from utils import string_to_date


def write_result_time_to_file(finall_time: float) -> None:
    """Запись времени выполнения кода в файл."""
    with open('result_time_sync.txt', 'w') as file:
        file.write(f'Sync code execution time: {finall_time}.\n')


def get_html_content(url: str) -> str | None:
    """Получение HTML."""
    try:
        response = requests.get(url)
        return response.text
    except Exception as e:
        print(e, 'Соединение разорвано, ещё одна попытка!')
        return None


def extract_xls_links(html) -> list[str]:
    """Поиск и сохранение в список ссылок на скачивание."""
    regular = r'href="(/upload/reports/oil_xls/oil_xls_\d{14}\.xls\?r=\d{4})"'
    links_for_download_xls = []

    for line in html.split('\n'):
        if 'href="/upload/reports/oil_xls/' in line:
            match = re.search(regular, line.strip())
            if match:
                filename = match.group(1).split('/')[-1]
                links_for_download_xls.append(
                    '/upload/reports/oil_xls/' + filename
                )

    return links_for_download_xls


def download_file(url: str, filename: str) -> bool:
    """Скачивание и сохранение xls файла."""
    while True:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                with open(filename, 'wb') as file:
                    file.write(response.content)
                return True
        except Exception as e:
            print(e, 'Соединение разорвано (скачивание документа), ещё раз!')


@contextmanager
def get_session():
    session = Session()
    try:
        yield session
        session.commit()
    finally:
        session.close


def save_to_database(row_data: list, year: int, month: int,
                     day: int) -> None:
    """Сохранение данных в базу данных."""
    with get_session() as session:
        new_data = Spimex_trading_results(
                    exchange_product_id=row_data[1],
                    exchange_product_name=row_data[2],
                    oil_id=row_data[1][:4],
                    delivery_basis_id=row_data[1][4:7],
                    delivery_basis_name=row_data[3],
                    delivery_type_id=row_data[1][-1],
                    volume=row_data[4],
                    total=row_data[5],
                    count=row_data[14],
                    date=datetime.date(year, month, day)
                )
        session.add(new_data)


def process_xls_file(filename: str) -> int:
    """Парсинг xls файла."""
    workbook = open_workbook(filename)
    sheet = workbook.sheet_by_index(0)
    valid_data = False
    year = 1978

    for row_idx in range(sheet.nrows):
        row_data = sheet.row_values(row_idx)
        if re.match(r'Дата торгов: \d{2}\.\d{2}\.\d{4}', row_data[1]):
            year, month, day = string_to_date(row_data[1][13:])
            if year == 2022:
                break
        if row_data[1] in SKIP_WORDS:
            continue
        if row_data[1] == 'Маклер СПбМТСБ':
            break
        if valid_data:
            if row_data[14] == '-':
                continue
            save_to_database(row_data, year, month, day)
        if 'Единица измерения: Метрическая тонна' in row_data[1]:
            valid_data = True
    return year


def main():
    """Логика парсинга."""
    parse = True
    page = 1

    while parse:
        url = URL + f'{page}'
        print(url)
        html = get_html_content(url)

        if not html:
            continue

        xls_links = extract_xls_links(html)

        for link in xls_links:
            full_url = 'https://spimex.com' + link
            filename = f'{link[-4:len(link)]}.xls'
            if download_file(full_url, filename):
                year = process_xls_file(filename)
                os.remove(filename)

                if year == 2022:
                    parse = False
                    break
        page += 1


if __name__ == "__main__":
    start_time = time.time()
    main()
    write_result_time_to_file(time.time() - start_time)
