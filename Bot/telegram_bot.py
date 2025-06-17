import asyncio
from dotenv import load_dotenv, find_dotenv
import os
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

from Bot.hes_vacancy import Hash_Vacancy
from Bot.parser_hh import ZarplataParser

load_dotenv(find_dotenv('.env'))

token = os.getenv("TOKEN")

bot = Bot(token)
dp = Dispatcher()

# Список подписчиков, которым отправлять новые вакансии
subscribers = set()


# Клавиатура с основными командами
def get_main_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="/help"), KeyboardButton(text="/subscribe")],
            [KeyboardButton(text="/unsubscribe"), KeyboardButton(text="/latest")]
        ],
        resize_keyboard=True
    )
    return keyboard


@dp.message(CommandStart())
async def process_start_command(message: Message):
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
        '/subscribe - подписаться на новые вакансии\n'
        '/unsubscribe - отписаться от рассылки\n'
        '/latest - получить последние вакансии\n\n'
        'Бот будет присылать новые вакансии с Zarplata.ru',
        reply_markup=get_main_keyboard()
    )


@dp.message(Command(commands='subscribe'))
async def subscribe_user(message: Message):
    user_id = message.from_user.id
    if user_id not in subscribers:
        subscribers.add(user_id)
        await message.answer("✅ Вы подписались на рассылку новых вакансий!")
    else:
        await message.answer("ℹ️ Вы уже подписаны на рассылку.")


@dp.message(Command(commands='unsubscribe'))
async def unsubscribe_user(message: Message):
    user_id = message.from_user.id
    if user_id in subscribers:
        subscribers.remove(user_id)
        await message.answer("❌ Вы отписались от рассылки.")
    else:
        await message.answer("ℹ️ Вы не были подписаны на рассылку.")


@dp.message(Command(commands='latest'))
async def send_latest_vacancies(message: Message):
    await message.answer("⏳ Ищу последние вакансии...")
    try:
        async for vacancies in ZarplataParser.get_vacancies():
            hasher = Hash_Vacancy(vacancies)
            added = hasher.process()
            if added:
                for vacancy in added[:5]:  # Отправляем первые 5 чтобы не спамить
                    await message.answer(format_vacancy(vacancy))
                break
            else:
                await message.answer("Новых вакансий не найдено.")
                break
    except Exception as e:
        await message.answer(f"⚠️ Ошибка при получении вакансий: {str(e)}")


def format_vacancy(vacancy):
    """Форматирует вакансию в читаемый вид"""
    return (f"🏢 {vacancy.get('company', 'Не указано')}\n"
            f"🔹 {vacancy.get('title', 'Без названия')}\n"
            f"💵 {vacancy.get('salary', 'З/п не указана')}\n"
            f"📍 {vacancy.get('location', 'Локация не указана')}\n"
            f"🔗 {vacancy.get('link', '#')}")


async def check_new_vacancies():
    """Периодически проверяет новые вакансии и рассылает подписчикам"""
    while True:
        try:
            async for vacancies in ZarplataParser.get_vacancies():
                hasher = Hash_Vacancy(vacancies)
                added = hasher.process()
                if added and subscribers:
                    for vacancy in added:
                        formatted = format_vacancy(vacancy)
                        for user_id in subscribers:
                            try:
                                await bot.send_message(user_id, formatted)
                                await asyncio.sleep(0.1)  # Чтобы не превысить лимиты Telegram
                            except Exception as e:
                                print(f"Не удалось отправить сообщение {user_id}: {e}")
                await asyncio.sleep(60 * 30)  # Проверяем каждые 30 минут
        except Exception as e:
            print(f"Ошибка при проверке вакансий: {e}")
            await asyncio.sleep(60 * 5)  # При ошибке ждем 5 минут перед повторной попыткой


async def main():
    # Запускаем фоновую задачу проверки вакансий
    asyncio.create_task(check_new_vacancies())

    # Запускаем бота
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())