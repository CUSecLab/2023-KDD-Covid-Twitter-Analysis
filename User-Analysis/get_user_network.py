import os
import gzip
import json
from tqdm import tqdm
from tokenize import group


# get user retweet network
for year in range(2019, 2023):
    year = str(year)
    for month in range(1,13):
        month = "%02d" % (month,)
        print(year + '-' + month)
        filenames = os.listdir(year + '-' + month)
        filenames.sort()
        with tqdm(total = len(filenames)) as pbar:
            for hour in filenames:
                x = pbar.update(1)
                if 'jsonl.gz' not in hour:
                    continue
                try:
                    tweets = gzip.open(year + '-' + month + '/' + hour).read().decode().split('\n')[:-1]
                    tweets = [json.loads(tweet) for tweet in tweets]
                except:
                    continue
                with gzip.open('www' + '/retweet/' + year + '-' + month + '-retweet.jsonl.gz', 'w') as f:
                    for tweet in tweets:
                        if tweet['lang'] != 'en':
                            continue
                        if 'retweeted_status' not in tweet.keys():
                            continue
                        retweet_user = {}
                        retweet_user[tweet['user']['screen_name']] = tweet['retweeted_status']['user']['screen_name']
                        x = f.write((json.dumps(retweet_user) + '\n').encode())
                        del retweet_user
                        del tweet
                del tweets



# get user reply network

def get_reply_user(text):
    users = []
    words = text.split()
    for word in words:
        if '@' in word:
            users.append(word.replace("\"",'').replace('@',''))
    return users

for year in range(2019, 2023):
    year = str(year)
    for month in range(1,13):
        # for past twetes, we don't have retweet data
        if year == '2020' and month < 10:
            continue
        month = "%02d" % (month,)
        print(year + '-' + month)
        filenames = os.listdir(year + '-' + month)
        filenames.sort()
        with tqdm(total = len(filenames)) as pbar:
            for hour in filenames:
                x = pbar.update(1)
                if 'jsonl.gz' not in hour:
                    continue
                try:
                    tweets = gzip.open(year + '-' + month + '/' + hour).read().decode().split('\n')[:-1]
                    tweets = [json.loads(tweet) for tweet in tweets]
                except:
                    continue
                with gzip.open('www' + '/reply/' + year + '-' + month + '-reply.jsonl.gz', 'w') as f:
                    for tweet in tweets:
                        if tweet['lang'] != 'en':
                            continue
                        if 'retweeted_status' in tweet.keys():
                            continue
                        reply_user = {}
                        reply_user[tweet['user']['screen_name']] = get_reply_user(tweet['text'])
                        x = f.write((json.dumps(reply_user) + '\n').encode())
                        del reply_user
                        del tweet
                del tweets



### get retweet times and put in one file

import os
import gzip
import json
from tqdm import tqdm

# put all retweets times together

files = os.listdir('user_retweet')
files.sort()
times = {}
for month in files:
    if '.txt' not in month:
        continue
    print(month)
    data = open('user_retweet/' + month).read().split('\n')[:-1]
    with tqdm(total = len(data)) as pbar:
        for j in data:
            x = pbar.update(1)
            source, target, time = j.split('\t')
            time = int(time)
            if time > 1:
                try:
                    times[(source, target)] = times[(source, target)] + time
                except:
                    times[(source, target)] = time
    del data



with tqdm(total = len(times)) as pbar:
    with open('user_retweet/times_all.txt', 'w') as f:
        for i in times:
            x = pbar.update(1)
            if times[i] > 1:
                x = f.write(i[0] + '\t' + i[1] + '\t' + str(times[i]) + '\n')


### get reply times to one file

reply_files = os.listdir('user_reply')
reply_files.sort()

# get user id to user name from each month retweet

for file in reply_files:
    print(file)
    times = {}
    tweets = gzip.open('user_reply/' + file).read().decode().split('\n')[:-1]
    with tqdm(total = len(tweets)) as pbar:
        for tweet in tweets:
            x = pbar.update(1)
            tweet = json.loads(tweet)
            for key in tweet:
                for user in tweet[key]:
                    if (key, user) in times:
                        times[(key, user)] = times[(key, user)] + 1
                    else:
                        times[(key, user)] = 1
            del tweet
    with tqdm(total = len(times)) as pbar:
        with open('user_reply/times_' + file + '.txt', 'w') as f:
            for i in times:
                x = f.write(i[0] + '\t' + i[1] + '\t' + times[i] + '\n')
    del tweets
    del times


with tqdm(total = len(times)) as pbar:
    with open('user_reply/times_all.txt', 'w') as f:
        for i in times:
            x = pbar.update(1)
            if times[i] > 1:
                x = f.write(i[0] + '\t' + i[1] + '\t' + str(times[i]) + '\n')


## save replies finished
## ## draw figures for reply network
# first get edges more than 10 times


replies = open('user_reply/times_all.txt').read().split('\n')[:-1]
users = open('offensive_user_name.txt').read().split('\n')[:-1]


more_than_ten = []
with tqdm(total = len(replies)) as pbar:
    for reply in replies:
        x = pbar.update(1)
        data = reply.split('\t')
        data[2] = int(data[2])
        if data[2] > 10:
            more_than_ten.append(data)


with tqdm(total = len(more_than_ten)) as pbar:
    with open('user_reply/times_all_10_more.txt', 'w') as f:
        for i in more_than_ten:
            x = pbar.update(1)
            x = f.write(i[0] + '\t' + i[1] + '\t' + str(i[2]) + '\n')


## draw figures for reply network

import csv
import pandas as pd
import networkx as nx
from tqdm import tqdm
import community.community_louvain


replies = open('user_reply/times_all_10_more.txt').read().split('\n')[:-1]
# 4,269,440
users = open('offensive_user_name.txt').read().split('\n')[:-1]


# get times more than 20
temp = {}
for reply in replies:
    data = reply.split('\t')
    data[2] = int(data[2])
    if data[2] > 20:
        temp[(data[0], data[1])] = data[2]

replies = temp.keys()
1,670,777


df = pd.DataFrame(replies, columns = ['source','target'])
G = nx.from_pandas_edgelist(df, 'source', 'target')
G.number_of_nodes()
G.remove_edges_from(nx.selfloop_edges(G))
# get nodes with more than 10 edges
G_tmp = nx.k_core(G, 10)
G_tmp.number_of_nodes()
partition = community.community_louvain.best_partition(G_tmp)
partition1 = pd.DataFrame([partition]).T
partition1 = partition1.reset_index()
partition1.columns = ['names','group']
G_sorted = pd.DataFrame(sorted(G_tmp.degree, key=lambda x: x[1], reverse=True))
G_sorted.columns = ['names','degree']
G_sorted.head()
dc = G_sorted
combined = pd.merge(dc,partition1, how='left', left_on="names",right_on="names")
combined = combined.rename(columns={"names": "Id"})
edges = nx.to_pandas_edgelist(G_tmp)
nodes = combined['Id']
edges.to_csv("edges.csv")
combined.to_csv("nodes.csv")


# make some change to nodes file
nodes = []
with open('nodes.csv') as f:
    reader = csv.reader(f)
    for row in reader:
        nodes.append(row)

with open('nodes.csv', 'w', newline='') as csvfile:
    fieldnames = ['Id', 'offen', 'group']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    x = writer.writeheader()
    for node in nodes[1:]:
        if node[1] in users:
            x = writer.writerow({ 'Id': node[1], 'offen': 1, 'group': node[3] })
        else:
            x = writer.writerow({ 'Id': node[1], 'offen': 0, 'group': node[3] })


# make some change to edges file
edges = []
with open('edges.csv') as f:
    reader = csv.reader(f)
    for row in reader:
        edges.append(row)


with open('edges.csv', 'w', newline='') as csvfile:
    fieldnames = [ 'source', 'target']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    x = writer.writeheader()
    for edge in edges[1:]:
        if edge[1] == '':
            continue
        x = writer.writerow({ 'source' : edge[1], 'target' : edge[2]})


## draw figure for retweet analysis


retweets = open('user_retweet/times_all.txt').read().split('\n')[:-1]
# 64,255,950 edges > 1

more_than_ten = []
with tqdm(total = len(retweets)) as pbar:
    for reply in retweets:
        x = pbar.update(1)
        data = reply.split('\t')
        data[2] = int(data[2])
        if data[2] > 10:
            more_than_ten.append(data)


with tqdm(total = len(more_than_ten)) as pbar:
    with open('user_retweet/times_all_10_more.txt', 'w') as f:
        for i in more_than_ten:
            x = pbar.update(1)
            x = f.write(i[0] + '\t' + i[1] + '\t' + str(i[2]) + '\n')


retweets = open('user_retweet/times_all_10_more.txt').read().split('\n')[:-1]
# 7,055,410
users = open('offensive_user_name.txt').read().split('\n')[:-1]


# get times more than 20
temp = {}
for reply in retweets:
    data = reply.split('\t')
    data[2] = int(data[2])
    if data[2] > 20:
        temp[(data[0], data[1])] = data[2]

retweets = temp.keys()
# 20 => 3,183,059
# 50 => 900,292


df = pd.DataFrame(retweets, columns = ['source','target'])
G = nx.from_pandas_edgelist(df, 'source', 'target')
G.number_of_nodes()
# 20 => 756,111
# 50 => 301,520
G.remove_edges_from(nx.selfloop_edges(G))
# get nodes with more than 10 edges
G_tmp = nx.k_core(G, 10)
G_tmp.number_of_nodes()
# 20 => 77,416
# 50 => 20,167

partition = community.community_louvain.best_partition(G_tmp)
partition1 = pd.DataFrame([partition]).T
partition1 = partition1.reset_index()
partition1.columns = ['names','group']
G_sorted = pd.DataFrame(sorted(G_tmp.degree, key=lambda x: x[1], reverse=True))
G_sorted.columns = ['names','degree']
G_sorted.head()
dc = G_sorted
combined = pd.merge(dc,partition1, how='left', left_on="names",right_on="names")
combined = combined.rename(columns={"names": "Id"})
edges = nx.to_pandas_edgelist(G_tmp)
nodes = combined['Id']
edges.to_csv("edges.csv")
combined.to_csv("nodes.csv")


# make some change to nodes file
nodes = []
with open('nodes.csv') as f:
    reader = csv.reader(f)
    for row in reader:
        nodes.append(row)

with open('nodes.csv', 'w', newline='') as csvfile:
    fieldnames = ['Id', 'offen', 'group']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    x = writer.writeheader()
    for node in nodes[1:]:
        if node[1] in users:
            x = writer.writerow({ 'Id': node[1], 'offen': 1, 'group': node[3] })
        else:
            x = writer.writerow({ 'Id': node[1], 'offen': 0, 'group': node[3] })


# make some change to edges file
edges = []
with open('edges.csv') as f:
    reader = csv.reader(f)
    for row in reader:
        edges.append(row)


with open('edges.csv', 'w', newline='') as csvfile:
    fieldnames = [ 'source', 'target']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    x = writer.writeheader()
    for edge in edges[1:]:
        if edge[1] == '':
            continue
        x = writer.writerow({ 'source' : edge[1], 'target' : edge[2]})
