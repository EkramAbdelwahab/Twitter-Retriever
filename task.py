#Import the necessary methods from tweepy library
import  tweepy
from tweepy import OAuthHandler
from tweepy import Stream
import json
from textwrap import TextWrapper
from datetime import datetime
from elasticsearch import Elasticsearch
from tweepy.streaming import StreamListener


#Variables that contains the user credentials to access Twitter API
access_token = "1269103098-4QPGPr2Ydpeqpmsqs3QYJhoPhmcU54QgLMdA3hK"
access_token_secret = "fBLkw5rBrK7ajKoP8L8WriFT5oYRYuOrqvic70NS0jC8d"
consumer_key = "AXMS95hCDCsFvSpReFWG5XPo8"
consumer_secret = "8aAHTW57VaPnJ5nea0OotvOLv5kIVFurllSGxRvg2gOsxsR1fK"


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

es = Elasticsearch()
class StreamListener(tweepy.StreamListener):
    status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')

    def on_status(self, status):
        try:
            

            json_data = status._json
            #print json_data['text']

            es.create(index="idx_twp",
                      doc_type="twitter_twp",
                      body=json_data
                     )

        except Exception as e:
            print(e)
            pass

streamer = tweepy.Stream(auth=auth, listener=StreamListener())

#Fill with your own Keywords bellow
#terms = ['big data','cloud computig']

streamer.filter(track=['cloud computing','bigdata'])
