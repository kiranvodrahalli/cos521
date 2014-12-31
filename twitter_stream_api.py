import json
import codecs
from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener

with open('config') as f:
    ACCESS_TOKEN, ACCESS_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET = f.read().strip().split(',')


class Listener(StreamListener):
    def __init__(self, outfile):
        self.f = codecs.open(outfile, 'w', encoding='utf-8')

    def on_data(self, data):
        tweet = json.loads(data)
        if ('id' in tweet and 'created_at' in tweet and
            'text' in tweet and 'lang' in tweet and
            tweet['lang'] == 'en'):
            if '#' in tweet['text']:
                tweet_id = unicode(tweet['id'])
                tweet_timestamp = tweet['created_at']
                tag_tokens = tweet['text'].strip().split('#')[1:]
                tags = [token.split()[0].rstrip('?:!.,;') for token in tag_tokens]
                for tag in tags:
                    self.f.write(','.join([tweet_id, tweet_timestamp, tag+'\n']))
                #print tweet_id, tweet_timestamp, tweet['text']

    def on_error(self, status):
        print 'Error:', status


if __name__ == '__main__':
    l = Listener('tweet_data')
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    stream = Stream(auth, l)
    stream.sample()
