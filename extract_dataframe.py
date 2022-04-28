import json
import pandas as pd
from textblob import TextBlob
# import zipfile

# with zipfile.ZipFile("./Data/Economic_Twitter_Data.zip", "r") as zip_ref:
#     zip_ref.extractall("./Data")
    
    
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
    def find_created_time(self) -> list:
        return self.tweets_list["created_at"]
    
    def find_full_text(self) -> list:
        for i in self.tweets_list["text"]:
#             print(i)
            return i
    
    def find_source(self) -> list:
        return self.tweets_list["source"]

    def find_sentiments(self, text) -> list:

        return self.tweets_list[['polarity', 'subjectivity']]
    
    def find_lang(self) -> list:
        return self.tweets_list["lang"]
    

    def find_favourite_count(self) -> list:
        return self.tweets_list["favorite_count"]
    
    
    def find_retweet_count(self) -> list:
        return self.tweets_list["retweet_count"]
    
        
    def find_screen_name(self) -> str:
        for myList in mention:
            for item in myList:
                return item.get("screen_name")    
    

    def find_followers_count(self) -> list:
        followers_count = [x.get("followers_count") for x in tweet_list["user"]]
        return followers_count 
        

    def find_friends_count(self) -> list:
        friends_count = [x.get("friends_count") for x in tweet_list["user"]]
        return friends_count  
        

    def is_sensitive(self) -> list:
        try:
            is_sensitive = [x['possibly_sensitive'] for x in self.tweets_list]
        except KeyError:
            is_sensitive = None

        return is_sensitive 

    

    def find_hashtags(self) -> list:
        hashtags = [x.get("hashtags") for x in tweet_list["entities"]]
        return hashtags
    

    def find_mentions(self) -> list:
        mentions = [x.get("user_mentions") for x in tweet_list["entities"]]
#         mentions
        return mentions


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
    _, tweet_list2 = read_json("")
    tweet_list = pd.DataFrame(tweet_list2).head(5)
    print(tweet_list["id"])
    tweet = TweetDfExtractor(tweet_list)
#     tweet.find_full_text()
#     tweet_df = tweet.get_tweet_df()
    location = [x.get("friends_count") for x in tweet_list["user"]]
#     mention = [x.get("user_mentions") for x in tweet_list["entities"]]
#     screen_names = [screen_name["screen_name"] for screen_name in mention]
    print(location)
    print("ext")
#     print(mention)
    
#     for myList in mention:
#         for item in myList:
#             print(item)