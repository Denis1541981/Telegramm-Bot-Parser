import asyncio
import logging
import sqlite3
from datetime import datetime
from dotenv import load_dotenv, find_dotenv
import os
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

from Bot.hes_vacancy import Hash_Vacancy
from Bot.parser_hh import ZarplataParser

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("vacancy_bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv(find_dotenv('.env'))
token = os.getenv("TOKEN")

bot = Bot(token)
dp = Dispatcher()


# Инициализация SQLite
def init_db():
    with sqlite3.connect('vacancy_bot.db') as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS subscribers (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                subscribed_at TIMESTAMP,
                filters TEXT
            )
        ''')
        conn.commit()


init_db()


# Клавиатура с основными командами
def get_main_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="/help"), KeyboardButton(text="/subscribe")],
            [KeyboardButton(text="/unsubscribe"), KeyboardButton(text="/latest")],
            [KeyboardButton(text="/set_filters"), KeyboardButton(text="/my_filters")]
        ],
        resize_keyboard=True
    )
    return keyboard


# Клавиатура пагинации
def get_pagination_keyboard(page: int, total_pages: int, prefix: str):
    builder = InlineKeyboardBuilder()

    if page > 1:
        builder.add(InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data=f"{prefix}_prev_{page - 1}"
        ))

    builder.add(InlineKeyboardButton(
        text=f"{page}/{total_pages}",
        callback_data="current_page"
    ))

    if page < total_pages:
        builder.add(InlineKeyboardButton(
            text="Вперёд ➡️",
            callback_data=f"{prefix}_next_{page + 1}"
        ))

    return builder.as_markup()


# Форматирование вакансии
def format_vacancy(vacancy):
    return (f"🏢 {vacancy.get('company', 'Не указано')}\n"
            f"🔹 {vacancy.get('title', 'Без названия')}\n"
            f"💵 {vacancy.get('salary', 'З/п не указана')}\n"
            f"📍 {vacancy.get('location', 'Локация не указана')}\n"
            f"🔗 {vacancy.get('link', '#')}")


# ========== Обработчики команд ==========
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


@dp.message(Command(commands='subscribe'))
async def subscribe_user(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or str(user_id)

    with sqlite3.connect('vacancy_bot.db') as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM subscribers WHERE user_id = ?', (user_id,))
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(
                'INSERT INTO subscribers (user_id, username, subscribed_at) VALUES (?, ?, ?)',
                (user_id, username, datetime.now())
            )
            conn.commit()
            logger.info(f"User {user_id} subscribed")
            await message.answer("✅ Вы подписались на рассылку новых вакансий!")
        else:
            await message.answer("ℹ️ Вы уже подписаны на рассылку.")


@dp.message(Command(commands='unsubscribe'))
async def unsubscribe_user(message: Message):
    user_id = message.from_user.id

    with sqlite3.connect('vacancy_bot.db') as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM subscribers WHERE user_id = ?', (user_id,))
        conn.commit()

        if cursor.rowcount > 0:
            logger.info(f"User {user_id} unsubscribed")
            await message.answer("❌ Вы отписались от рассылки.")
        else:
            await message.answer("ℹ️ Вы не были подписаны на рассылку.")


@dp.message(Command(commands='set_filters'))
async def set_filters_command(message: Message):
    await message.answer(
        "Введите ключевые слова для фильтрации вакансий (через запятую):\n"
        "Пример: Python, Django, удалёнка"
    )


@dp.message(F.text & ~F.text.startswith('/'))
async def process_filters(message: Message):
    user_id = message.from_user.id
    filters = message.text.strip()

    with sqlite3.connect('vacancy_bot.db') as conn:
        cursor = conn.cursor()
        cursor.execute(
            'UPDATE subscribers SET filters = ? WHERE user_id = ?',
            (filters, user_id)
        )
        conn.commit()

    logger.info(f"User {user_id} set filters: {filters}")
    await message.answer(f"✅ Фильтры обновлены: {filters}")


@dp.message(Command(commands='my_filters'))
async def show_filters(message: Message):
    user_id = message.from_user.id

    with sqlite3.connect('vacancy_bot.db') as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT filters FROM subscribers WHERE user_id = ?',
            (user_id,)
        )
        result = cursor.fetchone()

    filters = result[0] if result and result[0] else "не установлены"
    await message.answer(f"Ваши текущие фильтры: {filters}")


# Глобальная переменная для хранения вакансий между запросами
user_vacancies = {}


@dp.message(Command(commands='latest'))
async def send_latest_vacancies(message: Message):
    user_id = message.from_user.id
    await message.answer("⏳ Ищу последние вакансии...")

    try:
        # Получаем фильтры пользователя
        with sqlite3.connect('vacancy_bot.db') as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT filters FROM subscribers WHERE user_id = ?',
                (user_id,)
            )
            result = cursor.fetchone()
            filters = result[0].split(',') if result and result[0] else None

        # Получаем и фильтруем вакансии
        filtered_vacancies = []
        async for vacancies in ZarplataParser.get_vacancies():
            hasher = Hash_Vacancy(vacancies)
            added = hasher.process()

            if added:
                if filters:
                    for vacancy in added:
                        if any(keyword.lower() in vacancy.get('title', '').lower() or
                               keyword.lower() in vacancy.get('description', '').lower()
                               for keyword in filters):
                            filtered_vacancies.append(vacancy)
                else:
                    filtered_vacancies.extend(added)

        if filtered_vacancies:
            # Сохраняем вакансии для пагинации
            user_vacancies[user_id] = filtered_vacancies
            total_pages = len(filtered_vacancies) // 5 + 1

            # Отправляем первую страницу
            await send_vacancy_page(user_id, message.chat.id, 1)

        else:
            await message.answer("Новых вакансий по вашим фильтрам не найдено.")

    except Exception as e:
        logger.error(f"Error getting vacancies for {user_id}: {str(e)}")
        await message.answer(f"⚠️ Ошибка при получении вакансий: {str(e)}")


async def send_vacancy_page(user_id: int, chat_id: int, page: int):
    vacancies = user_vacancies.get(user_id, [])
    if not vacancies:
        await bot.send_message(chat_id, "Вакансии не найдены.")
        return

    total_pages = (len(vacancies) // 5) + 1
    page = max(1, min(page, total_pages))
    start_idx = (page - 1) * 5
    end_idx = start_idx + 5

    for vacancy in vacancies[start_idx:end_idx]:
        await bot.send_message(chat_id, format_vacancy(vacancy))

    if len(vacancies) > 5:
        await bot.send_message(
            chat_id,
            f"Страница {page} из {total_pages}",
            reply_markup=get_pagination_keyboard(page, total_pages, "vacancy")
        )


@dp.callback_query(F.data.startswith("vacancy_"))
async def handle_pagination(callback_query):
    data = callback_query.data
    user_id = callback_query.from_user.id
    chat_id = callback_query.message.chat.id

    if data.startswith("vacancy_prev_"):
        page = int(data.split("_")[2])
        await send_vacancy_page(user_id, chat_id, page)
    elif data.startswith("vacancy_next_"):
        page = int(data.split("_")[2])
        await send_vacancy_page(user_id, chat_id, page)

    await callback_query.answer()


async def check_new_vacancies():
    """Периодически проверяет новые вакансии и рассылает подписчикам"""
    while True:
        try:
            logger.info("Checking for new vacancies...")

            # Получаем всех подписчиков с их фильтрами
            with sqlite3.connect('vacancy_bot.db') as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT user_id, filters FROM subscribers')
                subscribers = cursor.fetchall()

            if not subscribers:
                await asyncio.sleep(60 * 30)
                continue

            # Получаем новые вакансии
            async for vacancies in ZarplataParser.get_vacancies():
                hasher = Hash_Vacancy(vacancies)
                added = hasher.process()

                if not added:
                    continue

                # Рассылаем каждому подписчику с учетом его фильтров
                for user_id, filters in subscribers:
                    filters_list = filters.split(',') if filters else None
                    sent_count = 0

                    for vacancy in added:
                        # Проверяем фильтры
                        if filters_list:
                            if not any(keyword.lower() in vacancy.get('title', '').lower() or
                                       keyword.lower() in vacancy.get('description', '').lower()
                                       for keyword in filters_list):
                                continue

                        try:
                            await bot.send_message(user_id, format_vacancy(vacancy))
                            sent_count += 1
                            await asyncio.sleep(0.1)  # Anti-flood

                            if sent_count >= 10:  # Лимит на одну рассылку
                                break

                        except Exception as e:
                            logger.error(f"Can't send to {user_id}: {e}")
                            break  # Прекращаем если пользователь заблокировал бота

            await asyncio.sleep(60 * 30)  # Проверяем каждые 30 минут

        except Exception as e:
            logger.error(f"Error in check_new_vacancies: {e}")
            await asyncio.sleep(60 * 5)  # При ошибке ждем 5 минут


async def main():
    # Запускаем фоновую задачу проверки вакансий
    asyncio.create_task(check_new_vacancies())

    # Запускаем бота
    logger.info("Starting bot...")
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())