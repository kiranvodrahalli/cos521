import sys
import json
import codecs

def parse_tweet_file(tweet_file):
    '''Writes a file of
            tweet_id,timestamp,hashtag
       for the tweets in the input json file.'''
    outfile = tweet_file.replace('.json', '.out')
    out_f = codecs.open(outfile, 'w', encoding='utf-8')
    with open(tweet_file) as f:
        for line in f:
            tweet = json.loads(line.strip())
            if ('id' in tweet and 'created_at' in tweet and
                'text' in tweet and 'lang' in tweet and
                tweet['lang'] == 'en'):
                if '#' in tweet['text']:
                    tweet_id = unicode(tweet['id'])
                    tweet_timestamp = tweet['created_at']
                    tag_tokens = tweet['text'].strip().split('#')[1:]
                    tags = [token.split()[0].rstrip('?:!.,;') for token in tag_tokens]
                    for tag in tags:
                        out_f.write(','.join([tweet_id, tweet_timestamp, tag+'\n']))

if __name__=='__main__':
    if len(sys.argv) != 2:
        print 'Usage: python tweet_parser.py <tweet_file.json>'
        sys.exit()
    parse_tweet_file(sys.argv[1])
