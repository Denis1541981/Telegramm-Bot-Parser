import asyncio
import logging
import re
from typing import Dict, AsyncGenerator, Any, Coroutine

from aiohttp import ClientSession
from bs4 import BeautifulSoup
from hes_vacancy import Hash_Vacancy


# Настройка логирования
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



def cleaner_str(string: str):

    cleaned_salary = re.sub(r'\xa0|\u202f|', ' ', string).strip()
    return cleaned_salary


class ZarplataParser:
    HEADERS = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
    }

    BASE_PARAMS = {
        'text': '',
        'excluded_text': '',
        'area': '1204',
        'salary': '',
        'currency_code': 'RUR',
        'experience': 'doesNotMatter',
        'employment': [
            'full',
            'part',
        ],
        'schedule': [
            'fullDay',
            'shift',
            'flexible',
            'remote',
            'flyInFlyOut',
        ],
        'order_by': 'relevance',
        'search_period': '0',
        'items_on_page': '100',
        'L_save_area': 'true',
        'hhtmFrom': 'vacancy_search_filter',
    }

    @staticmethod
    async def fetch_page(session: ClientSession, url: str, params: Dict) -> str | None:
        """Выполняет асинхронный запрос к странице"""
        try:
            async with session.get(url, headers=ZarplataParser.HEADERS, params=params) as response:
                if response.status == 200:
                    logger.info(f"Successful request to {url} with params {params}")
                    return await response.text()
                logger.error(f"Error: status {response.status} for page {params.get('page')}")
                return None
        except Exception as e:
            logger.error(f"Error requesting page {params.get('page')}: {str(e)}")
            return None


    @staticmethod
    async def get_vacancies() -> AsyncGenerator[Dict[str, Dict], None]:
        """Асинхронный генератор вакансий"""
        async with ClientSession() as session:
            for page in range(1):
                params = ZarplataParser.BASE_PARAMS.copy()
                params['page'] = page

                html = await ZarplataParser.fetch_page(session, 'https://berdsk.zarplata.ru/search/vacancy', params)
                if not html:
                    logger.info("HTML is None")
                    continue

                vacancies = ZarplataParser.parse_page(html)

                if vacancies:
                    logger.info(f"Found {len(vacancies)} vacancies on page {page}")
                    yield vacancies
                else:
                    logger.info(f"No vacancies found on page {page}")
                    yield {}
                await asyncio.sleep(1)  # Задержка между запросами

    @staticmethod
    def parse_page(html: str) -> Dict[str, Dict]:
        """Парсит HTML страницы и возвращает словарь вакансий"""
        soup = BeautifulSoup(html, "lxml")
        items = soup.find_all('div', class_='magritte-redesign')

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
    async for vacancies in ZarplataParser.get_vacancies():
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