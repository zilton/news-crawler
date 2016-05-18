# -*- coding: utf-8 -*-

'''
Created on May 03, 2016

@author: Zilton Cordeiro Junior
@email: ziltonjr@gmail.com

Collects and extracts information about news.
  

'''

import copy
import datetime
import feedparser
import hashlib
import random
import re
import time

from boilerpipe.extract import Extractor
from django.template.defaultfilters import pprint
import mechanize
from pymongo import Connection
import threading as t

TIME = 180 #Sleep time for the next verification on RSS
source = [] #Armazena todos os feeds de notícias

db_feed = None

connect_to_mongo = '200.131.6.200'

#Browser configuration
#Cria um navegador, um browser de código.
browser = mechanize.Browser()
# Ajusta algumas opções do navegador.
browser.set_handle_equiv(True)
browser.set_handle_gzip(False)
browser.set_handle_redirect(True)
browser.set_handle_referer(True)
browser.set_handle_robots(False)
browser.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)
# Configura o user-agent, para o servidor, o navegador é Firefox.
browser.addheaders = [('User-agent', 'Mozilla/5.0 (X11;\ U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615\Fedora/3.0.1-1.fc9 Firefox/3.0.1')]
    
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
        self.url_last_news = None

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
            
            print pprint(news)
                
            '''
            Controls the storage of the last news in the RSS.
            When this URL of the news is found, the monitoring of the RSS is aborted.
            '''
            if str(news['url']) != str(self.url_last_news):
                news_list.append(news)
                if news['text'] != 'TIMEOUT':#Was not possible to extract the text of the news.
                    news_list.append(news)
            else:
                break
        
    if len(news_list) > 0:
        del self.url_last_news
        self.url_last_news = copy.deepcopy(news_list[0]['url'])
        stores_news(self, news_list) #A lista de notícias é adicionada ao banco de dados
        
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
    
def stores_news(self, news_list = []):
    '''
        Stores the list of news on MongoDB.
        @news_list: Lista com todas as notícias a serem armazenadas
    '''
  
    #ConexaoBD.insereNoticiaMongoDB(news_list = [])

def connect_mongoDB():
    '''
        Opens connection to the mongoDB.
    '''
    
    global db_feed, connect_to_mongo
    
    error = True
    count_attempts = 0
    
    while error:
        try:
            count_attempts += 1
            mongoCon = Connection([connect_to_mongo])
            db_feed = mongoCon.feed
            print 'Connection opens on mongoDB: ' + connect_to_mongo + '\n\tBanco: feed\n\tData: ' + str(datetime.datetime.now())
            error = False
        except:
            if count_attempts == 1:
                print 'Não foi possível estabelecer conexão com o MongoDB em: ' + connect_to_mongo + '\n\tBanco: feed\n\tData: ' + str(datetime.datetime.now())
                print '\t\t--> Novas tentativas serão feitas a cada 10 minutos'
            error = True
            time.sleep(600)
    if count_attempts > 1:
        print '\tConexão estabelecida após %d tentativas.' % count_attempts

def init():
    '''
        Método de inicialização das Threads para coleta dos feeds.
        Acessa o MongoDB e coleta as informações dos feeds
    '''
    global source, TIME
    count_feeds = 0
    
    #listaFeed = ["http://g1.globo.com/dynamo/pr/parana/rss2.xml"]#ConexaoBD.coletaFeedInfo()
    listaFeed = [{"rss" : "http://g1.globo.com/dynamo/minas-gerais/rss2.xml", "city" : "Belo Horizonte"},
                 {"rss" : "http://www.correiobraziliense.com.br/rss/noticia/cidades/transito/rss.xml", "city" : "Brasilia"},
                 {"rss" : "http://g1.globo.com/dynamo/pr/parana/rss2.xml", "city" : "Curitiba"},
                 {"rss" : "http://g1.globo.com/dynamo/ceara/rss2.xml", "city" : "Fortaleza"},
                 {"rss" : "http://g1.globo.com/dynamo/am/amazonas/rss2.xml", "city" : "Manaus"},
                 {"rss" : "http://g1.globo.com/dynamo/rs/rio-grande-do-sul/rss2.xml", "city" : "Porto Alegre"},
                 {"rss" : "http://g1.globo.com/dynamo/pernambuco/rss2.xml", "city" : "Recife"},
                 {"rss" : "http://g1.globo.com/dynamo/rio-de-janeiro/rss2.xml", "city" : "Rio de Janeiro"},
                 {"rss" : "http://atarde.uol.com.br/arquivos/rss/transito.xml", "city" : "Salvador"},
                 {"rss" : "http://g1.globo.com/dynamo/bahia/rss2.xml", "city" : "Salvador"},
                 {"rss" : "http://g1.globo.com/dynamo/sao-paulo/rss2.xml", "city" : "Sao Paulo"}]
    
    for feed in listaFeed:
        source.append(ThreadCrawler(feed['rss'], feed['city']))
        count_feeds += 1        

    print "\tColetando " + str(count_feeds) + " feeds"    

if __name__ == "__main__":
    #print extract_by_boilerpipe(url = "http://g1.globo.com/ceara/noticia/2016/05/advogado-cobra-investigacao-sobre-lesoes-no-corpo-de-yrna-no-ceara.html")
    
    try:
        init()
            
        for fonte in source:
            fonte.start()#Inicializa todos os feeds
            time.sleep(5)
    except KeyboardInterrupt:
        print u'\nEncerrando...'
    