from collections import defaultdict
import itertools
from dateutil import parser

class NaiveTrendPredictor:
    def __init__(self, data_file):
        self.tweets = defaultdict(list)
        self.inverted_tweets_idx = defaultdict(list)

        with open(data_file) as f:
            for line in f:
                try:
                    id_, timestamp, hashtag = line.strip().split(',')
                    tweet_dt = parser.parse(timestamp)
                except Exception, e:
                    print e
                    continue
                self.tweets[hashtag.lower()].append(tweet_dt)
                self.inverted_tweets_idx[tweet_dt].append(hashtag.lower())

    def get_trending(self, n, start_dt, end_dt):
        hashtag_grps = (self.inverted_tweets_idx[tweet_dt]
                        for tweet_dt in self.inverted_tweets_idx
                        if start_dt <= tweet_dt and tweet_dt <= end_dt)
        most_popular = {(len(self.tweets[hashtag]), hashtag)
                        for hashtag_grp in hashtag_grps
                        for hashtag in hashtag_grp}
        return list(itertools.islice(reversed(sorted(most_popular)), 0, n))
