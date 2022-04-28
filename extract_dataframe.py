import json
import pandas as pd
from textblob import TextBlob
# from wordcloud import WordCloud
import numpy as np
import re
import matplotlib.pyplot as plt
plt.style.use('fivethirtyeight')
import zipfile

with zipfile.ZipFile("./Data/Economic_Twitter_Data.zip", "r") as zip_ref:
    zip_ref.extractall("./Data")
    
    
def read_json(json_file: str) -> list:
    """
    json file reader to open and read json files into a list
    Args:
    -----
    json_file: str - path of a json file

    Returns
    -------
    length of the json file and a list of json
    """

    tweets_data = []
    for tweets in open(json_file, 'r'):
        tweets_data.append(json.loads(tweets))

    return len(tweets_data), tweets_data



class TweetDfExtractor:
    """
    this function will parse tweets json into a pandas dataframe

    Return
    ------
    dataframe
    """

    def __init__(self, tweets_list):

        self.tweets_list = tweets_list

    # an example function
    def find_statuses_count(self) -> list:
        statuses_count = [x.get("statuses_count") for x in tweet_list["user"]]
        for statuses_count in statuses_count:
            return statuses_count
        
    def find_created_time(self) -> list:
        created_at = [x.get("created_at") for x in tweet_list["user"]]
        for created_at in created_at:
            return created_at
    
    def find_full_text(self) -> list:
        for i in self.tweets_list["text"]:
#             print(i)
            return i
    
    def find_source(self) -> list:
        return self.tweets_list["source"]

    def find_sentiments(self) -> list:
        newText = []
        for tweet in self.tweets_list["text"]:
#             print(tweet)
            text = re.sub(r'@[A-Za-z0-0]+', ' ', tweet)
            text = re.sub(r'#', '', text)
            text = re.sub(r':', '', text)
            text = re.sub(r'_', '', text)
            text = re.sub(r'RT[\s]+', ' ', text)
            text = re.sub(r'https:\/\/\S+', ' ', text)
            newText.append(text)
            print(newText)

        for i in text:
            polarity = TextBlob(i).sentiment.polarity
            subjectivity = TextBlob(i).sentiment.subjectivity
            #print(subjectivity)
#             print('next')
#             print(polarity)
            #print(i)
        print(type(text))
        
        
#     print(text)

#         return self.tweets_list[['polarity', 'subjectivity']]
    
    def find_lang(self) -> list:
        return self.tweets_list["lang"]
    

    def find_favourite_count(self) -> list:
        return self.tweets_list["favorite_count"]
    
    
    def find_retweet_count(self) -> list:
        return self.tweets_list["retweet_count"]
    
        
    def find_screen_name(self) -> str:
        for myList in mention:
            for item in myList:
                print(item.get("screen_name")  )   
    
    def find_followers_count(self) -> list:
        followers_count = [x.get("followers_count") for x in tweet_list["user"]]
        for followers_count in followers_count:
            return followers_count
#             print(followers_count)
            
        

    def find_friends_count(self) -> list:
        friends_count = [x.get("friends_count") for x in tweet_list["user"]]
        for friends_count in friends_count:
            return friends_count
#             print(followers_count)
        

    def is_sensitive(self) -> list:
        try:
            is_sensitive = self.tweets_list['possibly_sensitive']
        except KeyError:
            is_sensitive = None

        return is_sensitive 

    

    def find_hashtags(self) -> list:
        hashtags = [x.get("hashtags") for x in tweet_list["entities"]]
        return hashtags
    

    def find_mentions(self) -> list:
        mentions = [x.get("user_mentions") for x in tweet_list["entities"]]
        for mention in mentions:
            print(mention)
#         mentions
#         return mentions


    def find_location(self) -> list:
        try:
            location = [x.get("location") for x in self.tweets_list["user"]]
        except TypeError:
            location = ''
        return location
    
    
    def get_tweet_df(self, save=False) -> pd.DataFrame:
        """required column to be generated you should be creative and add more features"""

        columns = ['created_at', 'source', 'original_text', 'polarity', 'subjectivity', 'lang', 'favorite_count',
                   'retweet_count',
                   'original_author', 'followers_count', 'friends_count', 'possibly_sensitive', 'hashtags',
                   'user_mentions', 'place']

        created_at = self.find_created_time()
        source = self.find_source()
        text = self.find_full_text()
        polarity, subjectivity = self.find_sentiments(text)
        lang = self.find_lang()
        fav_count = self.find_favourite_count()
        retweet_count = self.find_retweet_count()
        screen_name = self.find_screen_name()
        follower_count = self.find_followers_count()
        friends_count = self.find_friends_count()
        sensitivity = self.is_sensitive()
        hashtags = self.find_hashtags()
        mentions = self.find_mentions()
        location = self.find_location()
        data = zip(created_at, source, text, polarity, subjectivity, lang, fav_count, retweet_count, screen_name,
                   follower_count, friends_count, sensitivity, hashtags, mentions, location)
        df = pd.DataFrame(data=data, columns=columns)

        if save:
            df.to_csv('processed_tweet_data.csv', index=False)
            print('File Successfully Saved.!!!')

        return df


if __name__ == "__main__":
    # required column to be generated you should be creative and add more features
    _, tweet_list2 = read_json("./data/Economic_Twitter_Data.json")
    tweet_list = pd.DataFrame(tweet_list2)
    tweet = TweetDfExtractor(tweet_list)
    tweet.find_sentiments()
#     tweet_df = tweet.get_tweet_df()


    
