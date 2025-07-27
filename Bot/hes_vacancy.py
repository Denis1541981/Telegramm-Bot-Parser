import json
import os
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)
handler = logging.FileHandler("hes_vacancy.log", 'a', encoding='utf-8')
logger.addHandler(handler)
stream_handler = logging.StreamHandler()
logger.addHandler(stream_handler)

logger.setLevel("INFO")



class Hash_Vacancy:
    def __init__(self, items: Dict[str, Any]=None):
        self.items = items
        self.new_vacancies: Dict[str, Any] = {}
        self.existing_data: Dict[str, Any] = self.load_existing_data()

    @classmethod
    def load_existing_data(cls) -> Dict[str, Any]:
        """Загружает существующие данные из файла"""
        file_path = "Vacancy.json"

        try:
            if os.path.exists(file_path):
                with open(file_path, "r", encoding='utf-8') as file:
                    return json.load(file)
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Error loading existing data: {e}")

        # Если файла нет или произошла ошибка
        with open(file_path, "w", encoding='utf-8') as file:
            json.dump({}, file)
        return {}

    def filter_new_vacancies(self):
        """Фильтрует вакансии, оставляя только новые"""
        for vacancy_id, vacancy_data in self.items.items():
            if vacancy_id not in self.existing_data:
                self.new_vacancies[vacancy_id] = vacancy_data
        return self.new_vacancies


    def save_new_update_vacancies(self):
        """Сохраняет новые вакансии и объединяет их с главным файлом, чтобы исключить дублирования объявлений в рассылке"""
        if not self.new_vacancies:
            return False

        # Объединяем старые и новые данные
        updated_data = {**self.existing_data, **self.new_vacancies}

        with open("Vacancy.json", "w") as f:
            json.dump(updated_data, f, indent=4, ensure_ascii=False)
        return True


    def process(self) -> dict:  # Всегда возвращаем словарь
        """Основной метод обработки вакансий"""


        if self.filter_new_vacancies():
            self.save_new_update_vacancies()
            logger.info(f"Найдено {len(self.new_vacancies)} вакансий")
        return self.new_vacancies


def main():

    with open("New_vacancies.json", "r") as f:
        x = json.load(f)
        print(len(x))
    hesh = Hash_Vacancy(x)
    z = hesh.process()
    print(z)


if __name__=="__main__":
    main()

