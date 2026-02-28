from __future__ import annotations

import asyncio
import logging
import re
from typing import Dict, AsyncGenerator, Optional

from aiohttp import ClientSession
from bs4 import BeautifulSoup
from hes_vacancy import Hash_Vacancy


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def cleaner_str(string: str) -> str:
    """Очищает строку от неразрывных пробелов"""
    return re.sub(r'[\xa0\u202f]', ' ', string).strip()


class ZarplataParser:
    """Парсер вакансий с Zarplata.ru"""

    HEADERS = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
    }

    # Устойчивые CSS-селекторы (без хэшей классов)
    SELECTORS = {
        'item': 'div[class*="vacancy"]',
        'title': 'span[class*="title"], h3[class*="title"], a[class*="title"]',
        'salary': 'span[class*="salary"], p[class*="salary"]',
        'company': 'span[class*="company"], a[class*="company"]',
        'city': 'span[class*="city"], [class*="address"]',
        'link': 'a[href*="/vacancy/"]',
    }

    def __init__(self, city: str = 'berdsk', pages: int = 1):
        self.city = city
        self.pages = pages
        self.base_url = f'https://{city}.zarplata.ru/search/vacancy'
        self.base_params = {
            'text': '',
            'area': '1204',  # Новосибирская обл.
            'items_on_page': '100',
        }

    async def fetch_page(self, session: ClientSession, page: int) -> Optional[str]:
        """Выполняет асинхронный запрос к странице"""
        params = self.base_params.copy()
        params['page'] = page

        try:
            async with session.get(self.base_url, headers=self.HEADERS, params=params) as response:
                if response.status == 200:
                    logger.info(f"Request successful: page {page}")
                    return await response.text()
                logger.error(f"Error status {response.status} for page {page}")
                return None
        except Exception as e:
            logger.error(f"Error requesting page {page}: {e}")
            return None

    async def get_vacancies(self) -> AsyncGenerator[Dict[str, Dict], None]:
        """Асинхронный генератор вакансий"""
        async with ClientSession() as session:
            for page in range(self.pages):
                html = await self.fetch_page(session, page)
                if not html:
                    continue

                vacancies = self.parse_page(html)
                yield vacancies
                await asyncio.sleep(1)  # Задержка между запросами

    def parse_page(self, html: str) -> Dict[str, Dict]:
        """Парсит HTML страницы и возвращает словарь вакансий"""
        soup = BeautifulSoup(html, "lxml")

        # Пробуем разные селекторы
        selectors = [
            'div[class*="magritte-redesign"]',
            'div[data-qa="vacancy-serp__vacancy"]',
            'div[class*="vacancy"]',
        ]

        items = []
        for sel in selectors:
            items = soup.select(sel)
            if items:
                logger.info(f"Found {len(items)} items with: {sel}")
                break

        if not items:
            logger.warning("No vacancy items found")
            return {}

        logger.info(f"Found {len(items)} items on the page")

        vacancies = {}


        for item in items:
            try:
                job_title = item.find("span", class_="magritte-text___tkzIl_5-0-26").text.replace(" ", ' ')

                salary = item.find("span", class_="magritte-text___pbpft_3-0-46 magritte-text_style-primary___AQ7MW_3-0-46 magritte-text_typography-label-1-regular___pi3R-_3-0-46").text

                title_company = item.find("span", class_="magritte-text___pbpft_3-0-46 magritte-text_style-primary___AQ7MW_3-0-46 magritte-text_typography-label-3-regular___Nhtlp_3-0-46").text.replace(" ", ' ')
                city = item.find('span', class_="magritte-text___pbpft_3-0-46 magritte-text_style-primary___AQ7MW_3-0-46 magritte-text_typography-label-3-regular___Nhtlp_3-0-46").text.replace(" ", ' ')
                link = item.find('a', class_="magritte-link___b4rEM_5-0-26 magritte-link_mode_primary___l6una_5-0-26 magritte-link_style_neutral___iqoW0_5-0-26 magritte-link_enable-visited___Biyib_5-0-26").get('href')

                if not link:
                    continue

                id_vacancy = link.split('/')[-1].split('?')[0]
                if not id_vacancy.isdigit():
                    continue

                vacancies[id_vacancy] = {
                    'Должность': job_title,
                    'Зарплата': cleaner_str(salary),
                    'Компания': title_company,
                    'Адрес': city,
                    'Ссылка': link
                }

            except (AttributeError, IndexError) as ex:
                logger.debug(f"Error parsing vacancy: {ex}")

        logger.info(f"Parsed {len(vacancies)} vacancies")
        return vacancies


async def main():
    total_added = 0
    parser = ZarplataParser(pages=1)
    async for vacancies in parser.get_vacancies():
        logger.info(f"Processing vacancies: {vacancies}")
        hasher = Hash_Vacancy(vacancies)
        added = hasher.process()
        if added:
            total_added += len(added)
            print(added)
        await asyncio.sleep(0.5)
    if total_added > 0:
        logger.info(f"Total new vacancies added: {total_added}")
    else:
        logger.info("No new vacancies found.")

if __name__ == "__main__":
    asyncio.run(main())