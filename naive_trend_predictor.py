from collections import defaultdict
import itertools
from dateutil import parser

TRENDING_THRESHOLD = 50

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


    def get_hashtags(self, start_dt, end_dt):
        hashtag_grps = (self.inverted_tweets_idx[tweet_dt]
                        for tweet_dt in self.inverted_tweets_idx
                        if start_dt <= tweet_dt and tweet_dt <= end_dt)
        hashtags = (hashtag for hashtag_grp in hashtag_grps
                    for hashtag in hashtag_grp)
        return hashtags


    def get_hashtag_freq(self, hashtag, start_dt, end_dt):
        return sum(start_dt <= tweet_dt and tweet_dt <= end_dt
                   for tweet_dt in self.tweets[hashtag])


    def get_most_popular(self, n, start_dt, end_dt):
        hashtags = self.get_hashtags(start_dt, end_dt)
        most_popular = {(self.get_hashtag_freq(hashtag, start_dt, end_dt), hashtag)
                        for hashtag in hashtags}
        return list(itertools.islice(reversed(sorted(most_popular)), 0, n))


    def get_most_novel(self, n, start_dt, end_dt):
        prev_start_dt = start_dt - (end_dt - start_dt)

        prev_hashtags = self.get_hashtags(prev_start_dt, start_dt)
        cur_hashtags = self.get_hashtags(start_dt, end_dt)

        prev_freq_counts = {hashtag: self.get_hashtag_freq(hashtag, prev_start_dt, start_dt)
                            for hashtag in prev_hashtags}
        cur_freq_counts = {hashtag: self.get_hashtag_freq(hashtag, start_dt, end_dt)
                           for hashtag in cur_hashtags}

        most_novel = (((cur_freq_counts[hashtag] + 1.0)/
                       (prev_freq_counts.get(hashtag, 0) + TRENDING_THRESHOLD), hashtag)
                      for hashtag in cur_freq_counts)

        return list(itertools.islice(reversed(sorted(most_novel)), 0, n))
