import requests
import time

from dotenv import load_dotenv, find_dotenv
import os



load_dotenv(find_dotenv('.env'))

BOT_TOKEN = os.getenv('TOKEN')

API_URL = 'https://api.telegram.org/bot'
API_CATS_URL = 'https://api.thecatapi.com/v1/images/search'

cat_response: requests.Response
cat_link: str
MAX_COUNTER = 100

offset = -2
counter = 0
chat_id: int
ERROR_TEXT = 'Здесь должна была быть картинка с котиком :('

# https://api.telegram.org/bot/<token>/sendMessage?chat_id=<chat_id>&text="AMAZING!"

while counter < MAX_COUNTER:

    print('attempt =', counter)  #Чтобы видеть в консоли, что код живет

    updates = requests.get(f'{API_URL}{BOT_TOKEN}/getUpdates?offset={offset + 1}').json()

    if updates['result']:
        for result in updates['result']:
            offset = result['update_id']
            chat_id = result['message']['from']['id']
            cat_response = requests.get(API_CATS_URL)
            if cat_response.status_code == 200:
                cat_link = cat_response.json()[0]['url']
                requests.get(f'{API_URL}{BOT_TOKEN}/sendPhoto?chat_id={chat_id}&photo={cat_link}')
            else:
                requests.get(f'{API_URL}{BOT_TOKEN}/sendMessage?chat_id={chat_id}&text={ERROR_TEXT}')

    time.sleep(1)


    counter += 1