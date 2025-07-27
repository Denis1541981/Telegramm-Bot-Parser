from typing import Dict
import logging

import numpy as np
import requests
import pandas as pd
from functools import lru_cache


logger = logging.getLogger(__name__)

@lru_cache(maxsize=100)
def get_search_city_id(city):
    r = requests.get("https://api.zarplata.ru/areas")
    if r.status_code != 200:
        raise Exception(f"API error: {r.status_code}")
    data = r.json()[0]
    for region in data['areas']:
        for area in region['areas']:
            if area['name'] == city:
                return area
    return None


def get_requests(city='Бердск', page=0, per_page=10, text=''):
    area = get_search_city_id(city)
    if not area:
        raise ValueError(f"Город {city} не найден")

    params = {
        'page': page,
        'per_page': per_page,
        'text': text,
        'area': area['id'],
    }
    r = requests.get("https://api.hh.ru/vacancies", params=params)
    if r.status_code != 200:
        raise Exception(f"Ошибка запроса: {r.status_code}")
    return r.json()


def parse_vacancy(content):
    try:
        return {
            'vacancy_id': content['id'],
            'vacancy_name': content['name'],
            'salary_from': content.get('salary', {}).get('from') if content['salary'] else np.nan,
            'address': content['address'].get('raw') if content.get('address') else None,
            'vacancy_url': content['alternate_url'],
            'employer_id': content['employer']['id'],
            'employer_name': content['employer']['name'],
            'employer_rating': content['employer']['employer_rating'].get('total_rating') if content['employer'].get('employer_rating') else None,
            'snippet_requirement': content['snippet'].get('requirement'),
            'snippet_responsibility': content['snippet'].get('responsibility'),
            'contacts': content.get('contacts'),
        }
    except Exception as e:
        print(f"Ошибка парсинга вакансии: {e}, content: {content}")
        return None


def parse_json(contents):
    if not contents or not contents.get("items"):
        return []
    parsed = []
    for item in contents["items"]:
        if not item:
            continue
        parsed_vacancy = parse_vacancy(item)
        if parsed_vacancy:
            parsed.append(parsed_vacancy)
    return parsed


def update_vacancy(new_df: pd.DataFrame) -> Dict:
    """Обновляет вакансии и возвращает новые в виде словаря"""
    try:
        # Загружаем старые вакансии
        try:
            old_df = pd.read_excel('Vacancies.xlsx')
            old_df.set_index('vacancy_id', inplace=True)
        except FileNotFoundError:
            old_df = pd.DataFrame()

        # Приводим индексы к числовому типу
        new_df.index = pd.to_numeric(new_df.index, errors='coerce')
        if not old_df.empty:
            old_df.index = pd.to_numeric(old_df.index, errors='coerce')

        # Находим новые вакансии
        missing_indexes = new_df.index.difference(old_df.index if not old_df.empty else pd.Index([]))

        if missing_indexes.empty:
            return {}

        update_df_vacacy = pd.concat([old_df,new_df], axis=0)
        update_df_vacacy.dropna(axis=0, how='all')
        update_df_vacacy.to_excel("Vacancies.xlsx")
        # Возвращаем новые вакансии в виде словаря
        return new_df.loc[missing_indexes].to_dict('index')
    except Exception as e:
        logger.error(f"Error in update_vacancy: {str(e)}", exc_info=True)
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
    df_new = pd.DataFrame(parse_json(get_requests(per_page=10, page=0, text='грузчик')))
    df_new.set_index('vacancy_id', inplace=True)
    x = update_vacancy(df_new)

    print(x)

if __name__ == "__main__":
    main()