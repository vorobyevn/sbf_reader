# -*- coding: utf-8 -*-

class WUtils:
    RENT_DAY = 1
    RENT_MONTH = 2

    def getMedia(self, media_dic, media_id):
        name = media_dic[media_id].lower()
        name = name.replace('.ru', '')
        if name == 'руки':
            name = 'irr'
        elif 'winner' in name:
            name = 'winner'
        elif name == 'яндекс':
            name = 'yandex'
        elif name == 'бн':
            name = 'bn'
        elif name == 'бкн':
            name = 'bkn'
        return name

    def getDicTowns(self, dic):
        return {k: v for k, v in [(s['id'], s['name']) for s in dic if s['is_metro'] == 0]}

    def getDicMetro(self, dic):
        return {k: v for k, v in [(s['id'], s['name']) for s in dic if s['is_metro'] == 1]}

    def getDicItem(self, dic, item_id):
        if item_id is None:
            return ''
        if item_id in dic:
            item = dic[item_id]
            return item
        # app.printError("%s %s not found"%(name, item_id))
        return None

    def getRentType(self, rent_type_id):
        # rt = self.getField(row, 'rent_type')
        if rent_type_id is None:
            return RENT_MONTH
        if rent_type_id == 3:
            return RENT_DAY
        else:
            return RENT_MONTH
        pass
# пусто 0
# любой срок 1
# длительный срок 2
# посуточно 3
# от месяца и более 4
# сезонная сдача 5

wUtils = WUtils()