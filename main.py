# -*- coding: utf-8 -*-
import pandas as pd
import requests
import time
from typing import Optional


def get_fio_by_inn(inn: str) -> Optional[str]:
    """
    Получает ФИО по ИНН через API ЕГРЮЛ налоговой службы.

    Args:
        inn: ИНН организации или ИП

    Returns:
        ФИО или None в случае ошибки
    """
    try:
        # Шаг 1: POST запрос для получения токена
        post_url = "https://egrul.nalog.ru/"
        post_data = {
            "vyp3CaptchaToken": "",
            "page": "",
            "query": inn,
            "region": "",
            "PreventChromeAutocomplete": ""
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

        response1 = requests.post(post_url, data=post_data, headers=headers, timeout=15)
        response1.raise_for_status()

        result1 = response1.json()
        token = result1.get("t")

        if not token:
            print(f"  Не удалось получить токен для ИНН {inn}")
            return None

        # Небольшая задержка между запросами
        time.sleep(1)

        # Шаг 2: GET запрос для получения данных
        import random
        random_param = random.randint(10000000000000, 99999999999999)
        get_url = f"https://egrul.nalog.ru/search-result/{token}?r={random_param}&_={inn}"

        response2 = requests.get(get_url, headers=headers, timeout=15)
        response2.raise_for_status()

        result2 = response2.json()
        rows = result2.get("rows", [])

        if not rows:
            print(f"  Данные не найдены для ИНН {inn}")
            return None

        # Извлекаем ФИО из поля "g" (ГЕНЕРАЛЬНЫЙ ДИРЕКТОР)
        first_row = rows[0]
        director_info = first_row.get("g", "")

        # Парсим строку вида "ГЕНЕРАЛЬНЫЙ ДИРЕКТОР: Жигарев Антон Вячеславович"
        if ":" in director_info:
            fio = director_info.split(":", 1)[1].strip()
            return fio

        return None

    except requests.exceptions.RequestException as e:
        print(f"  Ошибка при запросе к API для ИНН {inn}: {e}")
        return None
    except Exception as e:
        print(f"  Неожиданная ошибка для ИНН {inn}: {e}")
        return None


def process_excel_file(file_path: str):
    """
    Читает Excel файл, заполняет колонку с ФИО и сохраняет результат.

    Args:
        file_path: путь к Excel файлу
    """
    print(f"Чтение файла: {file_path}")

    # Читаем Excel файл
    df = pd.read_excel(file_path)

    print(f"Загружено строк: {len(df)}")
    print(f"Колонок в файле: {len(df.columns)}")
    print(f"\nНазвания колонок:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i}. {col}")

    # Проверяем, что есть хотя бы 5 колонок
    if len(df.columns) < 5:
        print("Ошибка: в файле меньше 5 колонок!")
        return

    # Получаем название 5-ой колонки (индекс 4, так как нумерация с 0)
    inn_column = df.columns[4]
    print(f"\nКолонка с ИНН: '{inn_column}'")

    # Создаем новую колонку для ФИО
    fio_column_name = "ФИО"

    # Проверяем, есть ли уже такая колонка
    if fio_column_name in df.columns:
        print(f"Колонка '{fio_column_name}' уже существует, будет перезаписана")
    else:
        print(f"Создание новой колонки '{fio_column_name}'")

    # Заполняем колонку ФИО
    fio_list = []

    for index, row in df.iterrows():
        inn = row[inn_column]

        # Проверяем, что ИНН не пустой
        if pd.isna(inn) or str(inn).strip() == "":
            print(f"Строка {index + 1}: ИНН пустой, пропускаем")
            fio_list.append("")
            continue

        print(f"Строка {index + 1}: Запрос ФИО для ИНН {inn}...")
        fio = get_fio_by_inn(str(inn))

        if fio:
            print(f"  Получено ФИО: {fio}")
            fio_list.append(fio)
        else:
            print(f"  ФИО не получено")
            fio_list.append("")

        # Небольшая задержка между запросами
        time.sleep(0.5)

    # Добавляем колонку с ФИО
    df[fio_column_name] = fio_list

    # Сохраняем результат
    output_file = file_path.replace(".xlsx", "_с_ФИО.xlsx")
    print(f"\nСохранение результата в: {output_file}")
    df.to_excel(output_file, index=False)
    print("Готово!")


if __name__ == "__main__":
    excel_file = "Список рассылки свод.xlsx"
    process_excel_file(excel_file)
