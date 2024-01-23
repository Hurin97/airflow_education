import requests

class Telegram_log:

    def __init__(self, cfg) -> None:
        self.token = cfg['telegram']['token']
        self.chat_id = cfg['telegram']['chat_id']

    def logger(self, message)->None:
        url = f"https://api.telegram.org/bot{self.token}/sendMessage?chat_id={self.chat_id}&text={message}"
        requests.get(url).json()