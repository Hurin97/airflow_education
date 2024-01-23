import db as db
# import postgresql
from telebot import Telegram_log as Tellog

class Dao:
    def __init__(self, cfg) -> None:
        self.tellog = Tellog(cfg)
        try:
            self.connection = db.Database(cfg).get_connection()
        except AttributeError as err:
            self.tellog.logger(f"Attribute Err :{err}")
        except db.postgresql.exceptions.ClientCannotConnectError as err:
            self.tellog.logger(f"Connection Err :{err}")

    def insert_into_table(self, table_name, fields, values) -> None:
        try:
            self.connection.execute("Insert into hstone." + table_name + "(" + fields + ")values" + values.replace('@@','\'\'').rstrip(','))
        except db.postgresql.exceptions.Error as err:
            self.tellog.logger(f"Err in insert :{err} \n {values}")

    def get_fields_table(self, table_name):
     res = []
     try:
         rows = self.connection.prepare(f"SELECT column_name FROM information_schema.columns WHERE table_name =\'{table_name}\'")
         for x in rows:
               res += x
     except db.postgresql.exceptions.Error as err:
        self.tellog.logger(f"Err in select fields:{err}")
     result = ','.join(res)
     return result