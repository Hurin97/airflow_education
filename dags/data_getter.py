# from os import path
import requests
import time
from telebot import Telegram_log as Tellog

class Data_getter:

    def __init__(self, cfg) -> None:
        # self.url = "https://omgvamp-hearthstone-v1.p.rapidapi.com/cards"
        # self.headers = {
	    # "X-RapidAPI-Key": "b3c49bc6camshf773d36a18e2923p1390abjsn585de42e1736",
	    # "X-RapidAPI-Host": "omgvamp-hearthstone-v1.p.rapidapi.com"
        # }
        self.url = cfg['api_g']['url']
        self.headers = cfg['api_g']['headers']  
        self.tellog = Tellog(cfg)


    def get_cards(self):
        print('Start getting cards from Heartstone')
        start_time = time.time()
        response = requests.get(self.url, headers=self.headers)
        if response.ok:
            finish_time = time.time()
            print(f'Finish process. Time:{finish_time - start_time}')
            return response
        else:
            self.tellog.logger(f"Empty response. Code:{response.status_code}")
            return "Empty response"
    
    def get_cards_in_json(self):
        print('Start getting cards from Heartstone')
        start_time = time.time()
        response = requests.get(self.url, headers=self.headers)
        if response.ok:
            data = response.json()
            finish_time = time.time()
            print(f'Finish process. Time:{finish_time - start_time}')
            return data
        else:
            self.tellog.logger(f"Empty response. Code:{response.status_code}")
            return "Empty response"        