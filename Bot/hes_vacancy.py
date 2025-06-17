import json
import os



class Hash_Vacancy:
    def __init__(self, items):
        self.items = items
        self.existing_data = self.load_existing_data()

    @classmethod
    def load_existing_data(cls):
        """Загружает существующие данные из файла"""

        file_path = "Vacancy.json"

        # Если файла нет, создаем пустой словарь
        if not os.path.exists(file_path):
            with open(file_path, "w") as f:
                json.dump({}, f)
            return {}

        with open(file_path, "r") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}

    def filter_new_vacancies(self):
        """Фильтрует вакансии, оставляя только новые"""
        new_vacancies = {}

        for vacancy_id, vacancy_data in self.items.items():
            if vacancy_id not in self.existing_data:
                new_vacancies[vacancy_id] = vacancy_data

        self.save_new_vacancies(new_vacancies)
        return new_vacancies

    @staticmethod
    def save_new_vacancies(new_vacancies):
        """Сохраняет новые вакансии для рассылки"""
        if not new_vacancies:
            return False
        with open("New_vacancies.json", "w") as f:
            json.dump(new_vacancies, f, indent=4, ensure_ascii=False)
        return True


    def save_new_update_vacancies(self, new_vacancies):
        """Сохраняет новые вакансии и объединяет их с главным файлом, чтобы исключить дублирования объявлений в рассылке"""
        if not new_vacancies:
            return False

        # Объединяем старые и новые данные
        updated_data = {**self.existing_data, **new_vacancies}

        with open("Vacancy.json", "w") as f:
            json.dump(updated_data, f, indent=4, ensure_ascii=False)

        return True

    def process(self):
        """Основной метод обработки вакансий"""
        new_vacancies = self.filter_new_vacancies()
        if new_vacancies:
            self.save_new_vacancies(new_vacancies)
            self.save_new_update_vacancies(new_vacancies)
            return new_vacancies
        return {}

