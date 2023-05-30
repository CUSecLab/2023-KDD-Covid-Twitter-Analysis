
import os
from tqdm import tqdm

usernames = open('offensive_user.txt').read().split('\n')[:-1]
usernames = [user.split('\t')[0] for user in usernames]


done = os.listdir('user_past_tweets')
with tqdm(total = len(usernames)) as pbar:
    for username in usernames:
        x = pbar.update(1)
        if username + '_2019.txt' in done:
            continue
        text = "snscrape --max-results 1000 twitter-user \'" + username + " since:2019-01-01- until:2019-12-31 \' >> user_past_tweets/" + username + "_2019.txt"
        x = os.system(text)
        text = "snscrape --max-results 1000 twitter-user \'" + username + " since:2020-01-01- until:2020-12-31 \' >> user_past_tweets/" + username + "_2020.txt"
        x = os.system(text)
        text = "snscrape --max-results 1000 twitter-user \'" + username + " since:2021-01-01- until:2021-12-31 \' >> user_past_tweets/" + username + "_2021.txt"
        x = os.system(text)


## get tweet id

x = os.system('mkdir user_past_tweets_data')
filenames = os.listdir('user_past_tweets')
for file in filenames:
    lines = open('user_past_tweets/' + file).read().split('\n')[:-1]
    ids = [line.split('/')[-1] for line in lines]
    with open('user_past_tweets_data/' + file, 'w') as f:
        for id in ids:
            x=f.write(id + '\n')


