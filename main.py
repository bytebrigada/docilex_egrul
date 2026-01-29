# -*- coding: utf-8 -*-
import signal
import sys
from typing import Dict, Optional

import pandas as pd
import requests

# Глобальный кеш для хранения соответствия ИНН -> ФИО
inn_cache: Dict[str, str] = {}

# Листы, где ИНН в колонке E (5-я, индекс 4)
SHEETS_WITH_INN_IN_E = [
    "Реестр поставщиков информации",
    # "Действующие",
    # "Действующие МФК",
    # "Действующие МКК",
    # "Исключенные",
]

# Глобальные переменные для сохранения при прерывании
current_file_path: str = ""
processed_sheets: Dict[str, pd.DataFrame] = {}
current_sheet_name: str = ""
current_df: Optional[pd.DataFrame] = None
interrupted: bool = False


def save_progress():
    """Сохраняет текущий прогресс в файл."""
    global processed_sheets, current_file_path, current_sheet_name, current_df

    if not current_file_path or not processed_sheets:
        print("\nНечего сохранять.")
        return

    # Если есть текущий обрабатываемый лист, добавляем его тоже
    if current_sheet_name and current_df is not None:
        processed_sheets[current_sheet_name] = current_df

    print(f"\n{'=' * 50}")
    print(f"Сохранение прогресса в файл: {current_file_path}")
    print(f"Сохраняемые листы: {list(processed_sheets.keys())}")

    try:
        with pd.ExcelWriter(current_file_path, engine="openpyxl") as writer:
            for sheet_name, df in processed_sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        print("Прогресс успешно сохранен!")
    except Exception as e:
        print(f"Ошибка при сохранении: {e}")


def signal_handler(signum, frame):
    """Обработчик сигнала прерывания (Ctrl+C)."""
    global interrupted
    interrupted = True
    print("\n\nПолучен сигнал прерывания (Ctrl+C)...")
    save_progress()
    print_cache_stats()
    sys.exit(0)


def print_cache_stats():
    """Выводит статистику кеша."""
    if inn_cache:
        print(f"\nСтатистика кеша:")
        print(f"  Уникальных ИНН обработано: {len(inn_cache)}")
        print(f"  Успешно найдено ФИО: {sum(1 for v in inn_cache.values() if v)}")
        print(f"  Не найдено: {sum(1 for v in inn_cache.values() if not v)}")


def get_inn_column_index(sheet_name: str) -> int:
    """
    Определяет индекс колонки с ИНН в зависимости от названия листа.

    Args:
        sheet_name: название листа

    Returns:
        Индекс колонки (4 для E, 5 для F)
    """
    if sheet_name in SHEETS_WITH_INN_IN_E:
        return 4  # Колонка E (индекс 4)
    else:
        return 5  # Колонка F (индекс 5)


def get_fio_by_inn(inn: str) -> Optional[str]:
    """
    Получает ФИО по ИНН через API ЕГРЮЛ налоговой службы.
    Сначала проверяет кеш, затем делает запрос к API.

    Args:
        inn: ИНН организации или ИП

    Returns:
        ФИО или None в случае ошибки
    """
    # Проверяем кеш
    if inn in inn_cache:
        cached_fio = inn_cache[inn]
        if cached_fio:
            print(f"    [КЕШ] Найдено: {cached_fio}")
        else:
            print(f"    [КЕШ] ИНН ранее не найден в ЕГРЮЛ")
        return cached_fio if cached_fio else None

    try:
        post_url = "https://egrul.nalog.ru/"
        post_data = {
            "vyp3CaptchaToken": "",
            "page": "",
            "query": inn,
            "region": "",
            "PreventChromeAutocomplete": "",
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }

        response1 = requests.post(post_url, data=post_data, headers=headers, timeout=15)
        response1.raise_for_status()

        result1 = response1.json()
        token = result1.get("t")

        if not token:
            print(f"    Не удалось получить токен")
            inn_cache[inn] = ""
            return None

        import random

        random_param = random.randint(10000000000000, 99999999999999)
        get_url = (
            f"https://egrul.nalog.ru/search-result/{token}?r={random_param}&_={inn}"
        )

        response2 = requests.get(get_url, headers=headers, timeout=15)
        response2.raise_for_status()

        result2 = response2.json()
        rows = result2.get("rows", [])

        if not rows:
            print(f"    Данные не найдены в ЕГРЮЛ")
            inn_cache[inn] = ""
            return None

        first_row = rows[0]
        director_info = first_row.get("g", "")

        if ":" in director_info:
            fio = director_info.split(":", 1)[1].strip()
            inn_cache[inn] = fio
            return fio

        inn_cache[inn] = ""
        return None

    except requests.exceptions.RequestException as e:
        print(f"    Ошибка API: {e}")
        return None
    except Exception as e:
        print(f"    Неожиданная ошибка: {e}")
        return None


def process_sheet(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    """
    Обрабатывает один лист Excel: добавляет колонку ФИО на основе ИНН.

    Args:
        df: DataFrame с данными листа
        sheet_name: название листа для логирования

    Returns:
        Обработанный DataFrame
    """
    global current_sheet_name, current_df

    current_sheet_name = sheet_name
    current_df = df

    print(f"\n{'=' * 50}")
    print(f"Обработка листа: '{sheet_name}'")
    print(f"Загружено строк: {len(df)}")
    print(f"Колонок в файле: {len(df.columns)}")

    # Определяем индекс колонки с ИНН
    inn_column_index = get_inn_column_index(sheet_name)
    column_letter = "E" if inn_column_index == 4 else "F"

    if len(df.columns) <= inn_column_index:
        print(
            f"Внимание: на листе '{sheet_name}' недостаточно колонок (нужна {column_letter}), пропускаем"
        )
        return df

    inn_column = df.columns[inn_column_index]
    print(
        f"Колонка с ИНН: '{inn_column}' (колонка {column_letter}, индекс {inn_column_index})"
    )

    fio_column_name = "ФИО"

    if fio_column_name in df.columns:
        print(f"Колонка '{fio_column_name}' уже существует, будет обновлена")
    else:
        print(f"Создание новой колонки '{fio_column_name}'")
        df[fio_column_name] = ""

    for index, row in df.iloc[27147:].iterrows():
        inn = row[inn_column]

        if pd.isna(inn) or str(inn).strip() == "":
            print(f"  Строка {index + 1}: ИНН пустой, пропускаем")
            df.at[index, fio_column_name] = ""
            continue

        inn_str = str(inn).strip()
        if inn_str.endswith(".0"):
            inn_str = inn_str[:-2]

        print(f"  Строка {index + 1}: ИНН {inn_str}...")
        fio = get_fio_by_inn(inn_str)

        if fio:
            print(f"    Получено: {fio}")
            df.at[index, fio_column_name] = fio
        else:
            print(f"    ФИО не получено")
            df.at[index, fio_column_name] = ""

        # Обновляем current_df для сохранения при прерывании
        current_df = df

    print(f"Лист '{sheet_name}' обработан успешно")
    return df


def process_excel_file(file_path: str):
    """
    Читает Excel файл со всеми листами, заполняет колонку ФИО
    и сохраняет изменения в исходный файл.

    Args:
        file_path: путь к Excel файлу
    """
    global current_file_path, processed_sheets

    current_file_path = file_path

    print(f"Чтение файла: {file_path}")
    print("Для прерывания с сохранением нажмите Ctrl+C\n")

    excel_file = pd.ExcelFile(file_path)
    sheet_names = excel_file.sheet_names

    print(f"Найдено листов: {len(sheet_names)}")
    print(f"Названия листов: {sheet_names}")

    # Загружаем все листы сразу (чтобы не потерять необработанные при сохранении)
    for sheet_name in sheet_names:
        processed_sheets[sheet_name] = pd.read_excel(file_path, sheet_name=sheet_name)

    # Обрабатываем каждый лист
    for sheet_name in sheet_names:
        df = processed_sheets[sheet_name]
        processed_df = process_sheet(df, sheet_name)
        processed_sheets[sheet_name] = processed_df

    print(f"\n{'=' * 50}")
    print(f"Сохранение изменений в файл: {file_path}")

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        for sheet_name, df in processed_sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    print_cache_stats()
    print("\nГотово! Все листы обработаны и сохранены.")


if __name__ == "__main__":
    # Регистрируем обработчик Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)

    excel_file = "Реестр поставщиков информации от  2026-01-26.xlsx"
    print(excel_file)
    process_excel_file(excel_file)
