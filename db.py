# -*- coding: utf-8 -*-
from bson.code import Code
import pymongo
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import app
import date_utils
from general_settings import generalSettings
from w_utils import wUtils
from datetime import datetime
from bson.objectid import ObjectId
import file_utils
import timeit

class DalW():
    db = None
    id_field = 'w6_offer_id'
    merge_field = 'merge_stat'
    load_field = 'load'
    addr_field = 'my_addr_id'

    def Db(self):
        if self.db == None:
            app.printInfo('Connect to database %s:%s' % (generalSettings.mongo_host, generalSettings.mongo_port))
            connection = MongoClient(generalSettings.mongo_host, generalSettings.mongo_port)
            self.db = connection[generalSettings.mongo_db]
            self.media = self.loadDic('dic_media')
        return self.db

    def convertRow(self, row):
        if row is None:
            return row
        if self.addr_field not in row:
            row[self.addr_field] = 0
        return row

    def convertPhoneRow(self, coll_name, row):
        new_win = False
        if 'w6_offer_id' in row:
            src_id = row['w6_offer_id']
            new_win = True
        else:
            src_id = row['deputy_id']
        ad = {}
        ad['src_name'] = coll_name
        ad['src_id'] = src_id
        if new_win:
            ad['phone'] = row['phone_list']
            ad['date'] = date_utils.fromTimestamp(row['pub_datetime'])
            ad['media'] = wUtils.getMedia(self.media, row['media_id'])
            ad['is_company'] = self.getIsCompany(ad['media'], row['broker_w5_realty_broker_id'])
        else:
            ad['phone'] = row['phs']
            ad['date'] = date_utils.fromTimestamp(row['updt'])
            ad['media'] = wUtils.getMedia(self.media, row['media_id'])
            ad['is_company'] = self.getIsCompany(ad['media'], row['broker_id'])
        return ad

    def createCollection(self, coll_name, auto_index_id = False):
        # self.Db().create_collection(coll_name, codec_options=None, read_preference=None, write_concern=None, read_concern=None, autoIndexId=auto_index_id)
        self.Db().create_collection(coll_name)
        # self.Db()[coll_name].create_index(self.id_field)
        # self.Db()[coll_name].create_index(self.merge_field)
        # self.Db()[coll_name].create_index(self.load_field)

    def createIndexes(self, coll_name):
        self.Db()[coll_name].create_index(self.id_field)
        self.Db()[coll_name].create_index(self.merge_field)
        self.Db()[coll_name].create_index(self.load_field)

    def dropCollection(self, coll_name):
        self.Db().drop_collection(coll_name)

    def getIds(self, coll_name):
        coll = self.Db()[coll_name]
        docs = coll.find({}, {'_id':1})
        return list(docs)

    def findIds(self, coll_name, condition):
        docs = self.Db()[coll_name].find(condition, {'_id':1})
        return list(docs)

    def getSrcIds(self, coll_name, id_field, limit=1000):
        coll = self.Db()[coll_name]
        docs = coll.find({}, {id_field:1, '_id': 0}).limit(limit)
        return [i[id_field] for i in list(docs)]

    def getRows(self, coll_name, skip_id=0, limit=500):
        docs = self.Db()[coll_name].find({'w6_offer_id': {'$gt': skip_id}, 'my_addr_id': {'$exists':0}}).sort([('w6_offer_id', 1)]).limit(limit)
        return list(docs)

    def getMergedRowIds(self, coll_name, status):
        docs = self.Db()[coll_name].find({self.merge_field: status, self.load_field: {'$exists':0}}, {'_id': 1})
        return [i['_id'] for i in list(docs)]

    def getRow(self, coll_name, obj_id):
        return self.Db()[coll_name].find_one({'_id': obj_id})

    def getPhoneRow(self, coll_name, obj_id):
        doc = self.Db()[coll_name].find_one({'_id': obj_id})
        return self.convertPhoneRow(coll_name, doc)

    def getRowBySrcId(self, coll_name, id_field, src_id):
        row = self.Db()[coll_name].find_one({id_field: src_id})
        return self.convertRow(row)

    def getDicRows(self, dic_name):
        coll = self.Db()[dic_name]
        rows = coll.find({}, {'_id':0}) #, 'id':1, 'name':1
        return list(rows)

    def loadDic(self, dic_name):
        rows = self.getDicRows(dic_name)
        return {k:v for k,v in [(s['id'],s['name']) for s in rows]}

    def isExistCollection(self, name):
        return name in self.Db().collection_names()

    def clearTable(self, coll_name):
        self.Db()[coll_name].drop()

    def count(self, coll_name):
        return self.Db()[coll_name].count()

    def getKeys(self, coll_name, limit = 0):
        if limit > 0:
            docs = self.Db()[coll_name].find().limit(limit)
        else:
            docs = self.Db()[coll_name].find()
        keys = []
        for doc in docs:
            kl = doc.keys()
            for k in kl:
                if k not in keys:
                    keys.append(k)
            pass
        return keys

    def updateRecBulk(self, coll_name, rec_buf, id_name):
        if len(rec_buf) <= 0:
            return (0, 0)
        bulk = self.Db()[coll_name].initialize_ordered_bulk_op()
        for rec in rec_buf:
            bulk.find( {id_name: rec[id_name]} ).upsert().update_one( {'$set': rec} )
        try:
            result = bulk.execute()
        except BulkWriteError as bwe:
            app.printError(bwe.details)
        return (result['nUpserted'], result['nModified'])

    def insertRecBulk(self, coll_name, rec_buf, id_name):
        count = len(rec_buf)
        if count == 0:
            return count
        bulk = self.Db()[coll_name].initialize_ordered_bulk_op()
        for rec in rec_buf:
            bulk.insert( rec )
        try:
            result = bulk.execute()
        except BulkWriteError as bwe:
            app.printError(bwe.details)
        return result['nInserted']

    def updateRec(self, coll_name, rec, id_name):
        if "_id" in rec:
            rec.pop("_id")
        self.Db()[coll_name].update({id_name: rec[id_name]}, rec, upsert=False)

    def insertRec(self, coll_name, rec, id_name):
        self.Db()[coll_name].update({id_name: rec[id_name]}, rec, upsert=True)

    def getNotMerged(self, coll_name, id_field, limit = 10000):
        recs = self.Db()[coll_name].find({self.merge_field: 's'}, {id_field: 1}) #.limit(limit)
        ids = [i[id_field] for i in list(recs)]
        return ids

    def clearMerge(self, coll_name):
        self.Db()[coll_name].update( {}, {'$set': {self.merge_field: 's'}}, multi=True)
        # self.Db()[coll_name].update( {self.merge_field: {'$exists':1}}, {'$set': {self.merge_field: 's'}}, multi=True)

    def saveMerge(self, coll_name, status, id_field, ids):
        if len(ids) == 0:
            return
        coll = self.Db()[coll_name]
        bulk = coll.initialize_ordered_bulk_op()
        for id in ids:
            bulk.find({id_field:id}).upsert().update_one({'$set': {self.merge_field:status}})
        try:
            result = bulk.execute()
        except BulkWriteError as bwe:
            app.printError(bwe.details)

    def getNotLoadedMerge(self, coll_name, id_field, merge_stat):
        # self.addr_field: {'$gt': 0}
        recs = self.Db()[coll_name].find({self.merge_field: merge_stat, self.load_field: {'$exists':0}}, {id_field: 1})
        return [i[id_field] for i in list(recs)]

    def updateField(self, coll_name, id_field, src_id, field, value):
        self.Db()[coll_name].update({id_field:src_id}, {'$set': {field:value}}, upsert=False)

    def updateRowsField(self, coll_name, id_field, ids, field, value):
        if len(ids) == 0:
            return (0, 0)
        bulk = self.Db()[coll_name].initialize_ordered_bulk_op()
        for id in ids:
            # bulk.update({id_field:id}, {'$set': {field:value}}, upsert=False)
            bulk.find( {id_field:id} ).upsert().update_one( {'$set': {field:value}} )
        try:
            result = bulk.execute()
        except BulkWriteError as bwe:
            app.printError(bwe.details)
        return (result['nUpserted'], result['nModified'])

    def deleteRows(self, coll_name, id_field, ids_del):
        for id in ids_del:
            self.Db()[coll_name].remove({id_field:id})

    def saveMergeChanged(self, coll_old, coll_new):
        self.Db().system_js.diff_changed = file_utils.readFile("js/diff_changed.js")
        self.Db().system_js.save_winner_modify = file_utils.readFile("js/save_modified.js")
        modify = total = 0
        iter_modify = 0
        iter_total = 1
        while iter_total > 0:
            start = timeit.default_timer()
            res = self.Db().system_js.save_winner_modify(coll_old, coll_new)
            iter_total = int(res['total'])
            iter_modify = int(res['modify'])
            total += iter_total
            modify += iter_modify
            stop = timeit.default_timer()
            app.printInfo("Save modify %s/%s, %s sec"%(modify, total, int(stop - start)))
        return modify

    def repairDatabase(self):
        self.Db().command("repairDatabase")

    def getIsCompany(self, media_id, broker_id):
        is_company = True
        if broker_id is None:
            broker_id = 0
        if media_id != 'winner' and broker_id == 0:
            is_company = False
        return is_company

dal = DalW()