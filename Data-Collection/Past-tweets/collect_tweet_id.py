# This file uses the snscrape tool to collect data ranging between two dates about a topic and saves the data to a file. 
# adjustments add more variables so there is less hard coding

import os
from datetime import *


def collectdata(keywordlist):
    """This function interacts with the terminal and gets data for every keyword over a period of time"""
    start_date = date(2020, 1, 1) 
    end_date = date(2022, 1, 1)
    delta = timedelta(days = 1)
    for keyword in keywordlist: 
        os.system("snscrape twitter-search '{2} since:{0}- until:{1} lang:en' >>twitter-datalinks-{2}.txt".format(start_date, end_date, keyword.replace(' ', '_'))) 


def extract_ids(keywordlist):
    """the purpose of this function is to collect the api keys from the twitter links"""
    for keyword in keywordlist: 
        twitterids = []
        file = open(f"twitter-datalinks-{keyword.replace(' ', '_')}.txt", 'r')
        tweets = file.readlines()
        for tweet in tweets:
            # extract api key
            temptweet = tweet.split("/")
            twitterids.append(temptweet[-1])
        outfile = open(f"twitter-ids-{keyword}.txt", 'w')
        outfile.write(''.join(twitterids))
        outfile.close()
        file.close()


def main():
    # after using this file run twitterids[keyword].txt in hydrate to get the csv form of the tweets
    # name the csv files twitterids{keyword}.csv
    keywordlist = ['microchip', 'fauci ', 'gates', '5g', 'curecoronavirus', 'curecovid', 'CoronavirusHoax']
    collectdata(keywordlist)
    extract_ids(keywordlist)


if __name__ == "__main__":
    main()

