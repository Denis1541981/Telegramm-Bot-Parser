import asyncio
import logging
import os
import re
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandStart
from aiogram.types import (InlineKeyboardButton, KeyboardButton, Message,
                           ReplyKeyboardMarkup, InlineKeyboardMarkup)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv, find_dotenv

import hh_ru

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("vacancy_bot.log", "a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv(find_dotenv('.env'))
TOKEN = os.getenv("TOKEN")
if not TOKEN:
    raise ValueError("TOKEN not found in .env file")

bot = Bot(token=TOKEN)
dp = Dispatcher()


# Инициализация SQLite
def init_db():
    with sqlite3.connect('vacancy_bot.db', check_same_thread=False) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS subscribers (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                subscribed_at TIMESTAMP,
                filters TEXT
            )
        ''')
        conn.commit()


init_db()


def get_main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="/help"), KeyboardButton(text="/subscribe")],
            [KeyboardButton(text="/unsubscribe"), KeyboardButton(text="/latest")],
            [KeyboardButton(text="/set_filters"), KeyboardButton(text="/my_filters")]
        ],
        resize_keyboard=True
    )


def format_vacancy(vacancies_dict: Dict) -> str:
    """
    Форматирует словарь вакансий для вывода
    с защитой от всех типов ошибок форматирования

    :param vacancies_dict: Словарь в формате {id: vacancy_data}
    :return: Отформатированная строка с вакансиями
    """
    if not vacancies_dict:
        return "Новых вакансий не найдено"

    result = []

    for vacancy_data in vacancies_dict.values():
        try:
            # Безопасное получение и форматирование зарплаты
            salary = vacancy_data.get('salary_from')

            if salary is None:
                cleaned_salary = 'З/п не указана'
            elif isinstance(salary, (int, float)):
                # Форматируем числовую зарплату
                cleaned_salary = f"{int(salary):,} ₽".replace(',', ' ')
            else:
                # Обрабатываем строковую зарплату
                cleaned_salary = re.sub(r'\u202f|\xa0', ' ', str(salary)).strip()

            # Безопасное получение остальных полей
            employer = str(vacancy_data.get('employer_name', 'Не указано'))
            position = str(vacancy_data.get('vacancy_name', 'Без названия'))
            address = str(vacancy_data.get('address', 'Локация не указана'))
            url = str(vacancy_data.get('vacancy_url', '#'))

            vacancy_str = (
                f"🏢 {employer}\n"
                f"🔹 {position}\n"
                f"💵 {cleaned_salary}\n"
                f"📍 {address}\n"
                f"🔗 {url}\n"
            )
            result.append(vacancy_str)

        except Exception as e:
            logger.error(f"Ошибка форматирования вакансии: {e}")
            continue

    return "\n".join(result) if result else "Нет вакансий для отображения"


def get_user_filters(user_id: int) -> List[str]:
    with sqlite3.connect('vacancy_bot.db') as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT filters FROM subscribers WHERE user_id = ?',
            (user_id,)
        )
        result = cursor.fetchone()
    return [f.strip().lower() for f in result[0].split(',')] if result and result[0] else []


def filter_vacancies(vacancies: Dict, filters: List[str]) -> Dict:
    if not filters:
        return vacancies

    filtered = {}
    for vacancy_id, vacancy_data in vacancies.items():
        vacancy_text = " ".join(str(v) for v in vacancy_data.values()).lower()
        if any(keyword in vacancy_text for keyword in filters):
            filtered[vacancy_id] = vacancy_data
    return filtered


async def get_new_vacancies(per_page=10, page=0, text=''):
    """Получение только новых вакансий с полной обработкой ошибок"""
    try:
        # Получаем и парсим данные
        parsed_data = pd.DataFrame(hh_ru.parse_json(hh_ru.get_requests(per_page=per_page, page=page, text=text)))
        parsed_data.set_index('vacancy_id', inplace=True)

        # Обновляем хранилище
        new_vacancies = hh_ru.update_vacancy(parsed_data)
        return new_vacancies

    except Exception as e:
        logger.error(f"get_new_vacancies error: {e}")
        return []


# Обработчики команд
@dp.message(CommandStart())
async def process_start_command(message: Message):
    logger.info(f"User {message.from_user.id} started the bot")
    await message.answer(
        'Привет! Я бот для отслеживания вакансий с Zarplata.ru.\n'
        'Используй /subscribe чтобы подписаться на новые вакансии.\n'
        'Используй /latest чтобы получить последние вакансии.\n'
        'Полный список команд: /help',
        reply_markup=get_main_keyboard()
    )


@dp.message(Command(commands='help'))
async def process_help_command(message: Message):
    await message.answer(
        '📌 Доступные команды:\n'
        '/start - запустить бота\n'
        '/help - список команд\n'
        '/subscribe - подписаться на рассылку\n'
        '/unsubscribe - отписаться от рассылки\n'
        '/latest - получить последние вакансии\n'
        '/set_filters - установить фильтры по ключевым словам\n'
        '/my_filters - посмотреть текущие фильтры',
        reply_markup=get_main_keyboard()
    )


@dp.message(Command(commands='latest'))
async def send_latest_vacancies(message: Message):
    user_id = message.from_user.id
    await message.answer("⏳ Ищу последние вакансии...")

    try:
        new_vacancies = await get_new_vacancies()
        if not new_vacancies:
            await message.answer("Новых вакансий не найдено.")
            return

        filters = get_user_filters(user_id)
        filtered_vacancies = filter_vacancies(new_vacancies, filters)

        formatted = format_vacancy(filtered_vacancies)
        await message.answer(formatted if formatted else "Нет вакансий по вашему фильтру.")

    except Exception as e:
        logger.error(f"Error getting vacancies for {user_id}: {e}")
        await message.answer("⚠️ Произошла ошибка при обработке вакансий")


@dp.message(Command(commands='subscribe'))
async def subscribe_user(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or str(user_id)

    with sqlite3.connect('vacancy_bot.db') as conn:
        cursor = conn.cursor()
        # Проверяем, не подписан ли уже пользователь
        cursor.execute('SELECT 1 FROM subscribers WHERE user_id = ?', (user_id,))
        if cursor.fetchone():
            await message.answer("Вы уже подписаны на рассылку вакансий.")
            return

        # Добавляем нового подписчика
        cursor.execute(
            'INSERT INTO subscribers (user_id, username, subscribed_at) VALUES (?, ?, ?)',
            (user_id, username, datetime.now())
        )
        conn.commit()

    await message.answer(
        "✅ Вы успешно подписались на рассылку новых вакансий!\n"
        "Используйте /set_filters чтобы настроить фильтры по ключевым словам.",
        reply_markup=get_main_keyboard()
    )
    logger.info(f"User {user_id} subscribed to vacancies")


@dp.message(Command(commands='unsubscribe'))
async def unsubscribe_user(message: Message):
    user_id = message.from_user.id

    with sqlite3.connect('vacancy_bot.db') as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM subscribers WHERE user_id = ?', (user_id,))
        conn.commit()

    if cursor.rowcount > 0:
        await message.answer(
            "Вы отписались от рассылки вакансий.",
            reply_markup=get_main_keyboard()
        )
        logger.info(f"User {user_id} unsubscribed from vacancies")
    else:
        await message.answer("Вы не были подписаны на рассылку.")


async def check_new_vacancies():
    """Периодически проверяет новые вакансии и рассылает подписчикам"""
    while True:
        try:
            logger.info("Checking for new vacancies...")
            new_vacancies = await get_new_vacancies()

            if not new_vacancies:
                await asyncio.sleep(60 * 30)
                continue

            with sqlite3.connect('vacancy_bot.db') as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT user_id, filters FROM subscribers')
                subscribers = cursor.fetchall()

            for user_id, filters in subscribers:
                try:
                    filters_list = [f.strip().lower() for f in filters.split(',')] if filters else []
                    filtered = filter_vacancies(new_vacancies, filters_list)

                    if filtered:
                        await bot.send_message(
                            user_id,
                            "Новые вакансии:\n" + format_vacancy(filtered)
                        )
                        await asyncio.sleep(0.1)
                except Exception as e:
                    logger.error(f"Error sending to user {user_id}: {str(e)}")

            await asyncio.sleep(60 * 30)

        except Exception as e:
            logger.error(f"Error in check_new_vacancies: {str(e)}")
            await asyncio.sleep(60 * 5)


async def main():
    asyncio.create_task(check_new_vacancies())
    logger.info("Starting bot...")
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())