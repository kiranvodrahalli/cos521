from datetime import datetime
import pytz
from naive_trend_predictor import NaiveTrendPredictor

ntp = NaiveTrendPredictor('tweet_data')
eastern = pytz.timezone('US/Eastern')
print ntp.get_most_popular(5, eastern.localize(datetime(2014, 12, 30, 16, 45)),
                       eastern.localize(datetime(2014, 12, 30, 17, 0)))
print ntp.get_most_novel(5, eastern.localize(datetime(2014, 12, 30, 16, 45)),
                       eastern.localize(datetime(2014, 12, 30, 17, 0)))
