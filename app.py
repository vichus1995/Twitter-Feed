import os
import tweepy
import json
import pyodbc
import pdb
from decouple import config

api_key = config('api_key')
api_secret =config('api_secret')
access_key = config('access_key')
access_secret = config('access_secret')
server = config('server')
db_name = config('db_name')
user = config('user')
password =config('password')

sql = '''EXEC dbo.Insert_Twitter_Data @tweetinfo =?'''

def insert_data(tweet_json):
    try:
        db_conn  = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+db_name+';UID='+user+';PWD='+ password)
        
        cursor = db_conn.cursor()
        cursor.execute(sql,tweet_json)
        cursor.commit()
    except pyodbc.Error as e:
        print(e)
    cursor.close()
    db_conn.close()
    

class myStreamListener(tweepy.StreamListener):
    def on_connect(self):
        print('Connected to Twitter')

    def on_error(self):
        if status_code != 200:
            print('Could not connect to Twitter')
            return False
    
    def on_data(self,data):
        try :
            raw_data = json.loads(data)
        
            if 'text' in raw_data:
                tweet ={}
                tweet['username'] = raw_data['user']['name']
                tweet['text']= raw_data['text']
                tweet['created_time']=raw_data['created_at']
                tweet['retweets_count']=raw_data['retweet_count']
                tweet['location']=raw_data['user']['location']
                tweet['place'] =raw_data['place']
                tweet_json = json.dumps(tweet)
                insert_data(tweet_json)
        except pyodbc.Error as e:
            print(e)


if __name__ =='__main__':

    auth = tweepy.OAuthHandler(api_key,api_secret)
    auth.set_access_token(access_key,access_secret)


    api = tweepy.API(auth,wait_on_rate_limit=True)

    listener = myStreamListener(api)
    stream = tweepy.Stream(auth,listener =listener)
    stream.filter(track = ['Football'], languages = ['en'])

    

    


