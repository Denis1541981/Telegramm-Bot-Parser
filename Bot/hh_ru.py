from typing import Dict, Optional
import logging
import time

import numpy as np
import requests
import pandas as pd
from functools import lru_cache


logger = logging.getLogger(__name__)

# Кэш для городов с временем жизни
_city_cache: Dict[str, tuple] = {}
CACHE_TTL = 3600  # 1 час


def get_search_city_id(city: str) -> Optional[Dict]:
    """Получает ID города с кэшированием"""
    current_time = time.time()

    # Проверяем кэш
    if city in _city_cache:
        cached_data, cached_time = _city_cache[city]
        if current_time - cached_time < CACHE_TTL:
            return cached_data

    # Запрос к API
    try:
        r = requests.get("https://api.hh.ru/areas", timeout=10)
        if r.status_code != 200:
            logger.error(f"API error: {r.status_code}")
            return _city_cache.get(city, (None, 0))[0]  # Возвращаем старое значение из кэша, если есть

        data = r.json()
        for country in data:
            for region in country.get('areas', []):
                for area in region.get('areas', []):
                    if area['name'] == city:
                        _city_cache[city] = (area, current_time)
                        return area
    except Exception as e:
        logger.error(f"Error fetching city ID: {e}")

    return None


def get_requests(city: str = 'Москва', page: int = 0, per_page: int = 10, text: str = '') -> Dict:
    """Получает вакансии с HH.ru API"""
    area = get_search_city_id(city)
    if not area:
        raise ValueError(f"Город {city} не найден")

    params = {
        'page': page,
        'per_page': per_page,
        'text': text,
        'area': area['id'],
    }

    try:
        r = requests.get("https://api.hh.ru/vacancies", params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logger.error(f"Request error: {e}")
        raise


def parse_vacancy(content: Dict) -> Optional[Dict]:
    """Парсит одну вакансию из API-ответа"""
    try:
        salary_data = content.get('salary')
        salary_from = salary_data.get('from') if salary_data else np.nan

        employer = content.get('employer', {})
        employer_rating = employer.get('employer_rating', {})

        return {
            'vacancy_id': content['id'],
            'vacancy_name': content['name'],
            'salary_from': salary_from,
            'address': content.get('address', {}).get('raw'),
            'vacancy_url': content['alternate_url'],
            'employer_id': employer.get('id'),
            'employer_name': employer.get('name'),
            'employer_rating': employer_rating.get('total_rating'),
            'snippet_requirement': content.get('snippet', {}).get('requirement'),
            'snippet_responsibility': content.get('snippet', {}).get('responsibility'),
            'contacts': content.get('contacts'),
        }
    except KeyError as e:
        logger.error(f"Missing key in vacancy: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing vacancy: {e}")
        return None


def parse_json(contents: Dict) -> list:
    """Парсит JSON-ответ от API"""
    if not contents or not contents.get("items"):
        return []

    return [
        parsed_vacancy
        for item in contents["items"]
        if item and (parsed_vacancy := parse_vacancy(item))
    ]


def update_vacancy(new_df: pd.DataFrame) -> Dict:
    """Обновляет вакансии и возвращает новые в виде словаря"""
    EXCEL_PATH = 'Vacancies.xlsx'

    try:
        # Загружаем старые вакансии
        try:
            old_df = pd.read_excel(EXCEL_PATH)
            old_df.set_index('vacancy_id', inplace=True)
        except FileNotFoundError:
            old_df = pd.DataFrame()

        # Приводим индексы к числовому типу
        new_df.index = pd.to_numeric(new_df.index, errors='coerce')
        if not old_df.empty:
            old_df.index = pd.to_numeric(old_df.index, errors='coerce')

        # Находим новые вакансии
        old_index = old_df.index if not old_df.empty else pd.Index([])
        missing_indexes = new_df.index.difference(old_index)

        if missing_indexes.empty:
            return {}

        # Обновляем Excel
        updated_df = pd.concat([old_df, new_df], axis=0)
        updated_df.dropna(axis=0, how='all', inplace=True)
        updated_df.to_excel(EXCEL_PATH)

        # Возвращаем новые вакансии
        return new_df.loc[missing_indexes].to_dict('index')

    except Exception as e:
        logger.error(f"Error in update_vacancy: {e}", exc_info=True)
        return {}


def get_all_vacancies(max_pages=30, per_page=50):
    vacancies = []
    for page in range(max_pages):
        data = parse_json(get_requests(per_page=per_page, page=page))
        if not data:
            break
        vacancies.extend(data)

    if vacancies:
        df = pd.DataFrame(vacancies)
        df.set_index('vacancy_id', inplace=True)
        df.to_excel("Vacancies.xlsx")
    return


def main():
    df_new = pd.DataFrame(parse_json(get_requests(per_page=10, page=0, text='Кладовщик')))
    df_new.set_index('vacancy_id', inplace=True)
    x = update_vacancy(df_new)

    print(x)

if __name__ == "__main__":
    main()