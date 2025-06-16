import asyncio
import logging
from typing import Dict, AsyncGenerator

from aiohttp import ClientSession
from bs4 import BeautifulSoup

# from Bot.hes_vacancy import Hash_Vacancy
from hes_vacancy import Hash_Vacancy  # убрали "Bot."

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ZarplataParser:
    HEADERS = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
    }

    BASE_PARAMS = {
        'text': '',
        'area': '1204',
        'experience': 'doesNotMatter',
        'order_by': 'relevance',
        'items_on_page': '10',
    }

    @staticmethod
    async def fetch_page(session: ClientSession, url: str, params: Dict) -> str:
        """Выполняет асинхронный запрос к странице"""
        try:
            async with session.get(url, headers=ZarplataParser.HEADERS, params=params) as response:
                if response.status == 200:
                    return await response.text()
                logger.error(f"Ошибка: статус {response.status} для страницы {params.get('page')}")
                return None
        except Exception as e:
            logger.error(f"Ошибка при запросе страницы {params.get('page')}: {str(e)}")
            return None

    @staticmethod
    async def get_vacancies() -> AsyncGenerator[Dict[str, Dict], None]:
        """Асинхронный генератор вакансий"""
        async with ClientSession() as session:
            for page in range(6):
                params = ZarplataParser.BASE_PARAMS.copy()
                params['page'] = page

                html = await ZarplataParser.fetch_page(session, 'https://www.zarplata.ru/search/vacancy', params)
                if not html:
                    continue

                vacancies = ZarplataParser.parse_page(html)
                if vacancies:
                    yield vacancies
                await asyncio.sleep(1)  # Задержка между запросами

    @staticmethod
    def parse_page(html: str) -> Dict[str, Dict]:
        """Парсит HTML страницы и возвращает словарь вакансий"""
        soup = BeautifulSoup(html, "lxml")
        items = soup.find_all('div', class_='magritte-redesign')
        logger.info(f"Найдено вакансий на странице: {len(items)}")

        vacancies = {}

        for item in items:
            try:
                job_title = item.find("span", class_="magritte-text___tkzIl_5-0-20").text.strip()
                salary = item.find("span",
                                   class_="magritte-text___pbpft_3-0-41 magritte-text_style-primary___AQ7MW_3-0-41 magritte-text_typography-label-1-regular___pi3R-_3-0-41").text.strip()
                title_company = item.find("span",
                                          class_="magritte-text___pbpft_3-0-41 magritte-text_style-primary___AQ7MW_3-0-41 magritte-text_typography-label-3-regular___Nhtlp_3-0-41").text.strip()
                city = item.find('span',
                                 class_="magritte-text___pbpft_3-0-41 magritte-text_style-primary___AQ7MW_3-0-41 magritte-text_typography-label-3-regular___Nhtlp_3-0-41").text.strip()
                link = item.find('a',
                                 class_="magritte-link___b4rEM_5-0-20 magritte-link_mode_primary___l6una_5-0-20 magritte-link_style_neutral___iqoW0_5-0-20 magritte-link_enable-visited___Biyib_5-0-20").get(
                    'href')

                if not link:
                    continue

                id_vacancy = link.split('/')[-1].split('?')[0]
                if not id_vacancy.isdigit():
                    continue

                vacancies[id_vacancy] = {
                    'Должность': job_title,
                    'Зарплата': salary,
                    'Компания': title_company,
                    'Адрес': city,
                    'Ссылка': link
                }
            except (AttributeError, IndexError) as ex:
                logger.debug(f"Ошибка парсинга вакансии: {ex}")
                continue

        logger.info(f"Успешно спарсено вакансий: {len(vacancies)}")
        return vacancies


async def main():
    total_added = 0
    async for vacancies in ZarplataParser.get_vacancies():
        hasher = Hash_Vacancy(vacancies)
        added = hasher.process()
        if added:
            total_added += len(added)
        await asyncio.sleep(0.5)
    if len(added) >= 1:
        logger.info(f"Всего добавлено новых вакансий: {len(added)}")

    logger.info(f"Новых вакансий пока нет.")


if __name__ == "__main__":
    asyncio.run(main())