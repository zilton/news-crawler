# -*- coding: utf-8 -*-

'''
Created on May 03, 2016

@author: Zilton Cordeiro Junior
@email: ziltonjr@gmail.com

Collects and extracts information about news.
  

'''

from boilerpipe.extract import Extractor
import copy
import datetime
import feedparser
import hashlib
import mechanize
from pymongo import Connection
import pymongo
import random
import re
import socket
import time

import threading as t

socket.setdefaulttimeout(300)

#Browser configuration
#Create a code browser
browser = mechanize.Browser()
browser.set_handle_equiv(True)
browser.set_handle_gzip(False)
browser.set_handle_redirect(True)
browser.set_handle_referer(True)
browser.set_handle_robots(False)
browser.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)

#Configure the user-agent for the server
browser.addheaders = [('User-agent', 'Mozilla/5.0 (X11;\ U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615\Fedora/3.0.1-1.fc9 Firefox/3.0.1')]

TIME = 180 #Sleep time for the next verification on RSS
source = []

mongo_db = None
connect_to_mongo = "mongodb1.ctweb.inweb.org.br"

class ThreadCrawler(t.Thread):
    '''
    Class that storage the thread information for each rss/feed. 
    '''
    
    def __init__(self, url_rss = "", city = ""):
        global TIME
        
        t.Thread.__init__(self)
        self.sleep_time = TIME
        self.url_rss = url_rss
        self.city = city
        self.id_last_news = None

    def run(self):
        '''
            Main loop for monitoring RSS news
        '''
        while True:                    
            collect_rss(self, self.url_rss, self.city)
                
            time.sleep(self.sleep_time) #Waiting time for the next access to the RSS repository 
               
def collect_rss(self, url_rss = None, city = None):
    '''
        Collect information about each news in the respective repository RSS.
        @url_rss: RSS URL that will be verify
        @city: The collected news has information about this city.
    '''    
                
    try:
        feed = feedparser.parse(url_rss).entries
    except:
        print 'Feed: ' + self.url_rss + ' - Problem to read - Attempts continue to be made.'
        return False
    
    news_list = []#Stores the collected news
    
    #Loop for collect the news
    for item in feed:
        time.sleep(random.randint(5,20))#Time to wait between news extractions.
        collect_date = datetime.datetime.now()
        
        published_date = datetime.datetime.strptime(item.published.replace(" -0300",""), '%a, %d %b %Y %H:%M:%S')
        
        #Stores the temporary URL. Is necessary treatment in case of URL that contains advertising for n seconds before redirect to the news page.
        news_url = item.link
        
        #Remove characters undesirable in the URL news
        try:
            news_url = eval(repr(news_url).replace('\\x91','\"').replace('\\x96', '-').replace('\\x97', '-').replace('\\x80','-').replace('\\x07','').replace('\\x92','\"').replace('\\x93','\"').replace('\\x94','\"').replace('\\x13','').replace('\\x14','').replace('\\x15','').replace('\\xa0','').replace('\\u2022','').replace('\\u0013','').replace('\\u0014','').replace('\\u2013','-').replace('\\r','').replace('\\t','').replace('\\n',''))
        except:
            news_url = eval(str(repr(news_url)).replace('\\x91','').replace('\\x96', '').replace('\\x97', '-').replace('\\x80','').replace('\\x07','').replace('\\x92','').replace('\\x93','').replace('\\x94','').replace('\\x13','').replace('\\x14','').replace('\\x15','').replace('\\xa0','').replace('\\u2022','').replace('\\u0013','').replace('\\u0014','').replace('\\u2013','-').replace('\\r','').replace('\\t','').replace('\\n',''))
                
        #Case the URL be a advertising, the mechanize redirect for the real URL news. 
        news_url = mechanize.urlopen(news_url).geturl()
            
        try:
            print 'City: ' + str(city) + ' -- TIME: ' + str(datetime.datetime.now()) + ' -- URL: ' + str(item.link.encode('utf8'))
        except:
            pass

        #Removes the date of the title of the news.
        ER = re.compile('^([0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9] - )',re.IGNORECASE)
        title = ER.sub('', item.title)
        ER = re.compile('^([A-Z]*: )',re.IGNORECASE)
        title = ER.sub('', title)
        
        #Gets the hash of the URL. THis will be the ID of the news.
        sha1 = hashlib.sha1()
        sha1.update(item.link.encode('utf8'))
        url_hash = sha1.hexdigest()
        
        tags = []
        try:
            tags = item.tags
        except:
            pass
        
        #Dictionary that define the news.
        news = {'text':None,
                '_id':url_hash,
                'city':city,
                'title':title,
                'url':news_url.encode('utf8'),
                'published':published_date,
                'collected':collect_date,
                'tags' : tags}
        
            
        news['text'] = extract_by_boilerpipe(url = news['url'])
            
        '''
        Controls the storage of the last news in the RSS.
        When this URL of the news is found, the monitoring of the RSS is aborted.
        '''
        if str(news['_id']) != str(self.id_last_news):
            if news['text'] != 'TIMEOUT':#Was not possible to extract the text of the news.
                news_list.append(news)
        else:
            break
        
    if len(news_list) > 0:
        del self.id_last_news
        self.id_last_news = copy.deepcopy(news_list[0]['url'])
        insert_news(self, news_list) #Stores the list of news on MongoDB.
        
    del news_list[:]
    del feed

def get_html(url = ""):
    '''
        Downloads the HTML of the news page.
        
        @url: URL to be extract.
        @return: The HTML of the URL.
    '''
    
    try:
        browser.open(url, timeout=45.0)# Access of the URL
        return browser.response().read()
    except:
        return ""

def extract_by_boilerpipe(url = ""):
    '''
        Extract the main text of the HTML (news).
        @url: URL to be extract.
        @return: text
    '''
    html = get_html(url=url)
    text = "TIMEOUT"
    cont = 0
    while cont <= 3:
        try:
            extractor = Extractor(extractor='ArticleExtractor', html=html)
            text = extractor.getText()
            break
        except:
            cont += 1
            time.sleep(5)       
    
    return text

def init():
    '''
        Method for initialize the threads for collects the RSSs.
    '''
    global source, TIME
    count_feeds = 0
    
    rss_list = collects_rss_info()
    
    for rss in rss_list:
        source.append(ThreadCrawler(rss['rss'], rss['city']))
        count_feeds += 1        

    print "\tCollecting " + str(count_feeds) + " RSSs"    

def connect_db():
    '''
        Opens connection at mongodb.
    '''
    
    global mongo_db, lucene, connect_to_mongo
    
    ERROR = True#Controls if occurs connection error
    count_attempts = 0#Number of attempts for connection
    
    while ERROR:
        try:
            count_attempts += 1
            mongoCon = Connection([connect_to_mongo])
            mongo_db = mongoCon.coleta_1000
            print 'Connection opened: ' + connect_to_mongo + '\n\tdb: coleta_1000\n\tDate: ' + str(datetime.datetime.now())
            ERROR = False
        except:
            if count_attempts == 1:
                print 'Was not possible opens connection: mongo ' + connect_to_mongo + '\n\tdb: coleta_1000\n\tDate: ' + str(datetime.datetime.now())
                print '\t\t--> New attempts will be made every 60 seconds'
            ERROR = True
            time.sleep(60)
            
    if count_attempts > 1:
        print '\tConnection established after %d attempts.' % count_attempts
        
            
def collects_rss_info():
    '''
        Collects the information about the RSSs to be monitoring.
    '''
    collection = mongo_db['rss_news']
    query = collection.find().sort('_id', pymongo.ASCENDING)
    rss_list = []
    
    for rss in query:
        rss_list.append(rss)
    
    return rss_list

def insert_news(self, news_list = []): 
    '''
        Inserts a news of news at the collection.
        @news_list: List of news to be storage. 
    '''
    global mongo_db
    
    news_to_be_inserted = []
    collection = mongo_db['news_rssNews']
    check_uniqueness = mongo_db['news_rssNews']
    
    for news in news_list:
        response = check_uniqueness.find({'_id':news['_id']},{'_id':1}).count()
        
        if response == 0:
            news_to_be_inserted.append(news)
    
    if len(news_to_be_inserted) > 0:
        collection.insert(news_to_be_inserted)
    
    
if __name__ == "__main__":    
    try:
        connect_db()
        init()
        
        for s in source:
            s.start()
            #time.sleep(5)
    except KeyboardInterrupt:
        print u'\nShutting down...'
    