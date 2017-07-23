import mysql.connector
import httplib
import hashlib
import time 

from pymongo import MongoClient


class MongoUrlManager:

    SERVER_IP = 'localhost'

    def __init__(self, client=None, expires=timedelta(days=30)):
        """
        client: mongo database client
        expires: timedelta of amount of time before a cache entry is considered expired
        """
        # if a client object is not passed 
        # then try connecting to mongodb at the default localhost port 
        self.client = MongoClient(self.SERVER_IP, 27017) if client is None else client
        #create collection to store cached webpages,
        # which is the equivalent of a table in a relational database
        self.db = self.client.spider

    def dequeueUrl(self):
        record = self.db.mfw.find_one_and_update(
            {'status': 'new'}, 
            { '$set': { 'status' : 'downloading'} }, 
            { 'upsert':False, 'returnNewDocument' : False} 
        )
        if record:
            return record
        else:
            return None

    def enqueuUrl(self, url, status, depth):
        record = {'status': status, 'queue_time': datetime.utcnow(), 'depth': depth}
        # self.db.webpage.update({'_id': url}, {'$set': record}, upsert=False)
        self.db.mfw.insert({'_id': url}, {'$set': record})

    def finishUrl(self, url):
        record = {'status': status, 'done_time': datetime.utcnow()}
        self.db.mfw.update({'_id': url}, {'$set': record}, upsert=False)

    def clear(self):
        self.db.mfw.drop()