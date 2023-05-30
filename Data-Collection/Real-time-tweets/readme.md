We collect real-time tweets with Twitter Streaming API.

The collected tweets would contains other languages and retweets, so we need to clean the tweets.

Since the tweet metadata contains too many attributes, we use the "clean-tweets.py" to clean tweets and only keep the most important attributes: id, text, user, time, and hashtags. You can adjust it by yourself if necessary.
