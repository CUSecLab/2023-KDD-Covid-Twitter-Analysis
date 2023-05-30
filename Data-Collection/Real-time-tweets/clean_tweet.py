import os
import json
import gzip

def get_text(tweet):
	try:
		text = tweet['extended_tweet']['full_text']
	except:
		text = tweet['text']
	return text


def get_hashtag(tweet):
	try:
		hashtags = tweet['extended_tweet']['entities']['hashtags']
	except:
		hashtags = tweet['entities']['hashtags']
	return [hashtag['text'] for hashtag in hashtags]


# for remove hashtags in text. If hashtag is in sentence, remove # symbol and keep text. Otherwise remove the hashtag.
def clean_text(text):
	words = text.split()
	words2 = []
	for word in words:
		if 'http' not in word and '\\' not in word:
			words2.append(word)
		if '\\u2019' in word:
			words2.append(word.replace('\u2019','\''))
	for word in words2[::-1]:
		if '#' in word:
			words2.remove(word)
		if '#' not in word:
			break
	sentence = ''
	for word in words2:
		sentence = sentence+i+' '
	sentence = sentence.replace('#','')
	return sentence


# this is for year 2022

year = '2022'
for month in range(1,13):
	month = "%02d" % (month,)
	files = os.listdir(year + '-' + month)
	try:
		done = os.listdir(year + '-' + month + '-clean')
	except:
		os.system('mkdir ' + year + '-' + month + '-clean')
	files.sort()
	for file in files:
		if 'jsonl.gz' not in file:
			continue
		if file in done:
			continue
		print(file)
		try:
			tweets = gzip.open(year + '-' + month + '/' + file).read().decode().split('\n')[:-1]
		except:
			f = open('log_clean.txt','a')
			x = f.write(file+' is broken!'+'\n')
			f.close()
			continue
		f = gzip.open(year + '-' + month + '-clean/' + file, 'a')
		f2 = gzip.open(year + '-' + month + '.jsonl.gz', 'a')
		for tweet in tweets:
			tweet = json.loads(tweet)
			if tweet['lang'] != 'en':
				continue
			if 'retweeted_status' in tweet.keys():
				continue
			cleaned_tweet = {}
			cleaned_tweet['text'] = clean_text(get_text(tweet))
			cleaned_tweet['id'] = tweet['id']
			cleaned_tweet['time'] = tweet['created_at']
			cleaned_tweet['user'] = tweet['user']['id']
			cleaned_tweet['hashtags'] = get_hashtag(tweet)
			x = f.write((json.dumps(cleaned_tweet) + '\n').encode())
			x = f2.write((json.dumps(cleaned_tweet) + '\n').encode())
		f.close()
		f2.close()
		del tweets
