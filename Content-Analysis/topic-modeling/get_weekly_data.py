import re
import gzip
import json
import nltk
import datetime
from tqdm import tqdm
start_time = datetime.datetime(2020,1,1)
stopwords = open('stopwords.txt').read().split('\n')[:-1]


for week in range(0, 130):    ## 2020/1/1 - 2021/6/30, 546 days, 78 weeks
    print('week-' + str(week))
    texts = []
    hashtags = []
    filename = 'weekly-tweets/week-' + str(week) + '.jsonl.gz'
    tweets = gzip.open(filename,'r').read().decode().lower().split('\n')[:-1]
    tweets = [json.loads(tweet) for tweet in tweets]
    for tweet in tweets:
        text = tweet['text']
        text = ' '.join([word for word in nltk.word_tokenize(re.sub(r'[^\w]', ' ', text)) if word not in stopwords])
        hashtag = ' '.join(tweet['hashtags'])
        text = text + ' ' +  hashtag
        text = text.replace('covid 19','covid19')
        text = text.replace('covid 19','covid19').replace('covid19','').replace('coronavirus','').replace('covid','')
        texts.append(text)
        hashtags.append(hashtag)
    del tweets
    if len(texts) == 0:
        continue
    with open('btm/sample-data/text/doc_info_week' + str(week) + '.txt','w') as f:
        for i in texts:
            x = f.write(i + '\n')


