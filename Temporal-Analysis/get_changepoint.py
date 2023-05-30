from changepy import pelt
from changepy.costs import normal_mean

data = open('daily_tweets.txt').read().split('\n')[:-1]

number = []
id_to_date = {}

for day in data:
    daily_id, date, daily_number = day.split('\t')
    number.append(float(daily_number))
    id_to_date[int(daily_id)] = date


changepoints = {}

threshold = 0
interval = 1
while True:
    threshold = threshold + interval
    result = pelt(normal_mean(number, threshold), len(number))
    for day in result:
        changepoints[day] = threshold
    if result == [0]:
        break
    if threshold == 100 * interval:
        interval = threshold/10


sorted_changepoints = sorted(changepoints.items(), key=lambda kv: kv[1],reverse=True)
for changepoint in sorted_changepoints[1:20]:
    print(id_to_date[changepoint[0]], changepoint[1])
