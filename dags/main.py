import sys
import yaml
from data_getter import Data_getter
from service import Service 
PATH_TO_PYTHON_BINARY = sys.executable

class Main:
    def __init__(self) -> None:
        pass

    def execute(self):
        try:
            with open('/opt/airflow/dags/config.yml') as c:
                config = yaml.safe_load(c)
        except OSError as err:
            print("OS error:{0}".format(err))
        cards = Data_getter(config).get_cards_in_json()
        if cards != "Empty response":
            Service(config).set_data_in_database(cards)