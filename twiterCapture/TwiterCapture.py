from elasticsearch import Elasticsearch
from twitter import *
import config
import tweepy
import time
import datetime
import json

class TwiterCapture:

    def __init__(self, api, es, hastag, index, debug=False):
        self.metadata = {
            'twets': {
                'old': {
                    'findItem': 0,
                    'news': 0
                },
                'new': {
                    'findItem': 0,
                    'news': 0
                }
            }
        }
        self.metadata_copy = {
            'twets': {
                'old': {
                    'findItem': 0,
                    'news': 0
                },
                'new': {
                    'findItem': 0,
                    'news': 0
                }
            }
        }
        self.hastag = hastag
        self.index = index
        self.api = api
        self.es = es
        self.debug = debug
        self.newsItems = 50
        self.newsItemsLimits = 10

    def getOldTwets(self, items):

        # get last ID
        id = self.es.getLastId(self.index)
        tewts = tweepy.Cursor(self.api.search, q=self.hastag, max_id=id, result_type='recent', count=items)
        news = self.es.insterTewtInES(tewts.items(items), self.index, self.hastag, self.debug)

        # metadata
        self.metadata['twets']['old']['findItem'] = items
        self.metadata['twets']['old']['news'] = news

        print('san afagit: ' + str(news) + ' vells')
        return news

    def getLastTwets(self, items):
        items += 1
        id = self.es.getFirsId(self.index)
        while True:
            tewts = tweepy.Cursor(self.api.search, q="#JudiciProcés", since_id=id, result_type='recent', count=items).items(items)
            news = self.es.insterTewtInES(tewts, self.index, self.hastag, True)
            if news < items:
                break
            if news == items:
                items = items * 10
        #metadata

        self.metadata['twets']['new']['news'] = news
        self.metadata['twets']['new']['findItem'] = items
        print('san afagit: ' + str(news) + ' nous')
        return news

    def createIdex(self):
        print(self.index not in self.es.getIdex())
        if self.index not in self.es.getIdex():
            tewts = tweepy.Cursor(self.api.search, q=self.hastag, result_type='recent', count=500)
            news = self.es.insterTewtInES(tewts.items(500), self.index, self.hastag, self.debug)
            print('inici amb: ' + str(news) + ' twets nous')
            time.sleep(60)

    def InitialCapture(self):
        oldItems = 500
        while True:
            #try:
                lastNewsOld = self.getOldTwets(500)

                if self.debug:
                    print('new limits: 500')
                    print('old = ' + str(oldItems))
                    print('slep 1 minute')
                    print('########################')
                self.es.insertMetadataInES(self.metadata, self.hastag, self.index)
                self.metadata = self.metadata_copy

                if lastNewsOld == 0:
                    break
                time.sleep(60)

    def logTimeCapture(self):
        print(datetime.datetime.now())
        try:
            #lastNewsNews = self.updateTwets(ids, newsItems)
            self.newsItems = self.getLastTwets(self.newsItemsLimits)
            self.newsItemsLimits = self.fineItems(self.newsItems, self.newsItemsLimits)
            if self.debug:
                print('new limits: ' + str(self.newsItemsLimits))
                print('new = ' + str(self.newsItems))
                print('slep 1 minute')
                print('########################')
            self.es.insertMetadataInES(self.metadata, self.hastag, self.index)
            self.metadata = self.metadata_copy
        except Exception as e:
            print(e)
            print('sleep 2 minutes')
            time.sleep(120)

    def updateTwets(self, ids, newsItems):


        if datetime.datetime.now().time() == datetime.time(3, 0, 0):
            tewts = self.es.search(index='twiter-judiciproces', doc_type='twet',
                                   body={"query": {"range": {"@timestamp": {"lte": "now-7d/d", "gte": "now-8d/d"}}},
                                         "sort": [{"id": {"order": "desc"}}]})
            for tewt in tewts['hits']['hits']:
                ids += [tewt['_id']]
            print(' tewts a actualisat: ' + str(len(ids)))
        if newsItems < 500 and len(ids) > 0:
            pass

    def fineItems(self, News, Items):
        minim = 10
        maxim = 500
        if News * 2 < Items:
            Items = Items / 2
            if Items < minim:
                Items = minim
        elif News >= (Items - (Items * 0.30)):
            Items = Items * 2
            if Items > maxim:
                Items = maxim
        return round(Items)

class Eleastic():

    def __init__(self, es):
        self.es = es

    def insterTewtInES(self, tweets, index, hastag, debug=True):
        i = 0
        debug = True
        print('es')
        print(tweets)
        for tweet in tweets:
            print(tweet)
            if self.es.exists(index=index, doc_type='twet', id=tweet._json['id']):
                if debug:
                    print('update last day ')
                self.es.update(index=index, doc_type='twet', id=tweet._json['id'],
                          body={"doc": self.filterJson(tweet._json, hastag)})
            else:
                if debug:
                    print('new last day')
                self.es.index(index=index, doc_type='twet', id=tweet._json['id'],
                              body=self.filterJson(tweet._json, hastag))
                i += 1

        return i

    def insertMetadataInES(self, data, hastag, index):
        data['@timestamp'] = (datetime.datetime.now() -datetime.timedelta(hours=1)).isoformat()
        data['Hastag'] = hastag
        self.es.index(index='metadata-' + index, doc_type='metadata', body=json.dumps(data))

    def getLastId(self, index):
        return self.es.search(index=index, doc_type='twet',
          body={"query": {"match_all": {}}, "sort": [{"id": {"order": "asc"}}], "size": 1})['hits']['hits'][0]['_id']

    def getFirsId(self, index):
        return self.es.search(index=index, doc_type='twet',
          body={"query": {"match_all": {}}, "sort": [{"id": {"order": "desc"}}], "size": 1})['hits']['hits'][0]['_id']

    def getIdex(self):
        return self.es.indices.get_alias("*")

    def filterJson(self, json, hastag):
        # create timestap
        json['@timestamp'] = datetime.datetime.strptime(json['created_at'], "%a %b %d %H:%M:%S +0000 %Y").isoformat()
        json['Hastag'] = hastag

        # delete indices
        if 'coordinates' in json:
            del json['coordinates']

        if 'contributors' in json:
            del json['contributors']

        if 'place' in json:
            del json['place']

        if 'quoted_status' in json:
            del json['quoted_status']

        return json

class TwiterScreduler():

    def __init__(self, period = 60):
        self.period = period

    def do_every(self, f):
        def g_tick():
            t = time.time()
            count = 0
            while True:
                count += 1
                yield max(t + count * self.period - time.time(), 0)

        g = g_tick()
        while True:
            time.sleep(next(g))
            f.logTimeCapture()

if __name__ == '__main__':
    # elastic serch
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    # twiter api
    twitter = Twitter(auth=OAuth(config.access_key,
                                 config.access_secret,
                                 config.consumer_key,
                                 config.consumer_secret))

    auth = tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)  # Interacting with twitter's API
    auth.set_access_token(config.access_key, config.access_secret)
    api = tweepy.API(auth)  # creating the API object

    # main program
    #time.sleep(60)

    database = Eleastic(es)
    twiterCaptureJudiciproces = TwiterCapture(api, database, '#JudiciProcés', 'twiter-judiciproces', debug=True)

    twiterCaptureJudiciproces.createIdex()
    twiterCaptureJudiciproces.InitialCapture()
    print('log time')
    twiterCaptureJudiciproces.logTimeCapture()
    while True:
        try:
            twiterScewduler = TwiterScreduler(60)
            twiterScewduler.do_every(twiterCaptureJudiciproces)
        except:
            pass
