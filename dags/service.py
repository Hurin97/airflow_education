import time
from dao import Dao

class Service:
    def __init__(self, cfg) -> None:
        self.config = cfg

    def if_none_to_null(self, func):
     if func == None:
          return 'null'
     return func
    
    def null_or_not(self, list:list, field_name):
     result = list.get(field_name)
     if result == None:
          return 'null'
     elif field_name == 'mechanics':
          text = ''
          for t in result:
               text += t.get('name') + ','
          return text.rstrip(',')
     elif field_name == 'runeCost':
          text = ''
          for x in result:
               text += x + ','
          return text.rstrip(',')
     else:
          return result
     
    def get_card_data(self, card):
     a = []
     a.append(self.if_none_to_null(card.get('cardId')))
     a.append(str(self.if_none_to_null(card.get('dbfId'))))
     a.append(str(self.if_none_to_null(card.get('name'))).replace('\'','@@'))
     a.append(self.if_none_to_null(card.get('cardSet')).replace('\'','@@'))
     a.append(self.if_none_to_null(card.get('type')))
     a.append(self.if_none_to_null(card.get('faction')))
     a.append(self.if_none_to_null(card.get('rarity')))
     a.append(str(self.if_none_to_null(card.get('cost'))))
     a.append(str(self.if_none_to_null(card.get('attack'))))
     a.append(str(self.if_none_to_null(card.get('health'))))
     a.append(self.if_none_to_null(card.get('text')).replace('\'','@@'))
     a.append(self.if_none_to_null(card.get('flavor')).replace('\'','@@'))
     a.append(self.if_none_to_null(card.get('artist')).replace('\'','@@'))
     a.append(self.if_none_to_null(str(card.get('collectible'))))
     a.append(self.if_none_to_null(str(card.get('elite'))))
     a.append(self.if_none_to_null(card.get('race')))
     a.append(self.if_none_to_null(card.get('playerClass')))
     a.append(self.if_none_to_null(card.get('img')))
     a.append(self.if_none_to_null(card.get('imgGold')))
     a.append(self.if_none_to_null(card.get('locale')))
     a.append(self.null_or_not(card, 'mechanics'))
     a.append(self.if_none_to_null(card.get('howToGet')).replace('\'','@@'))
     a.append(self.if_none_to_null(card.get('howToGetGold')).replace('\'','@@'))
     a.append(self.if_none_to_null(card.get('howToGetSignature')).replace('\'','@@'))
     a.append(self.if_none_to_null(card.get('howToGetDiamond')).replace('\'','@@'))
     a.append(self.null_or_not(card, 'runeCost'))
     return a

    def set_data_in_database(self, cards:dict):
        start_time = time.time()
        print('Start set data in db')
        rows = 0
        dao = Dao(self.config)
        fields = dao.get_fields_table('cards')
        values_insert = ''
        for key in cards:
            for x in cards[key]:
                if rows < 1000:
                    card = self.get_card_data(x)
                    values_insert += str(tuple(card)) + ','
                    rows += 1
                if rows == 1000:
                    dao.insert_into_table('cards', fields, values_insert)
                    rows = 0
                    values_insert = ''
        if rows > 0:
            dao.insert_into_table('cards', fields, values_insert)
        finish_time = time.time()
        print(f'Finish set dat. Time:{finish_time - start_time}')
