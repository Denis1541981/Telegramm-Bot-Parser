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

        if not os.path.exists(file_path):
            # Файл не существует - создаём пустой
            cls._save_data(file_path, {})
            return {}

        try:
            with open(file_path, "r", encoding='utf-8') as file:
                return json.load(file)
        except json.JSONDecodeError as e:
            logger.error(f"Error loading existing data (corrupted file): {e}")
            # Создаём резервную копию
            backup_path = f"{file_path}.backup"
            try:
                os.rename(file_path, backup_path)
                logger.info(f"Created backup: {backup_path}")
            except OSError:
                pass
            cls._save_data(file_path, {})
            return {}

    @staticmethod
    def _save_data(file_path: str, data: Dict) -> None:
        """Сохраняет данные в файл"""
        with open(file_path, "w", encoding='utf-8') as file:
            json.dump(data, file, indent=4, ensure_ascii=False)

    def filter_new_vacancies(self):
        """Фильтрует вакансии, оставляя только новые"""
        for vacancy_id, vacancy_data in self.items.items():
            if vacancy_id not in self.existing_data:
                self.new_vacancies[vacancy_id] = vacancy_data
        return self.new_vacancies


    def save_new_update_vacancies(self) -> bool:
        """Сохраняет новые вакансии и объединяет их с главным файлом"""
        if not self.new_vacancies:
            return False

        updated_data = {**self.existing_data, **self.new_vacancies}
        self._save_data("Vacancy.json", updated_data)
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

