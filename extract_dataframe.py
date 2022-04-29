import json
import pandas as pd
from textblob import TextBlob
import re
def read_json(json_file: str)->list:
    """
    json file reader to open and read json files into a list
    Args:
    -----
    json_file: str - path of a json file
    
    Returns
    -------
    length of the json file and a list of json
    """
    
    tweets_data=pd.read_json(json_file,lines=True)
    
    
    return  tweets_data

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
    def find_statuses_count(self)->list:
        
        tweets = pd.DataFrame(self.tweets_list)
        
        statuses_count=tweets["user"].get("statuses_count")
        

        return  statuses_countdf
        
    def find_full_text(self)->list:
        
        tweets = pd.DataFrame(self.tweets_list)
        
        text=tweets["text"]
        

        return  text
       
    def find_sentiments(self)->list:
                
        tweets = pd.DataFrame(self.tweets_list)
        newText = []
        for tweet in tweets["text"]:

            text = re.sub(r'@[A-Za-z0-0]+', ' ', tweet)
            text = re.sub(r'#', '', text)
            text = re.sub(r':', '', text)
            text = re.sub(r'_', '', text)
            text = re.sub(r'RT[\s]+', ' ', text)
            text = re.sub(r'https:\/\/\S+', ' ', text)
            newText.append(text)

            polarity=[]
            subjectivity=[]

        for i in newText:
            polarity_txt = TextBlob(i).sentiment.polarity
                        
            polarity.append(polarity_txt)

            subjectivity_txt = TextBlob(i).sentiment.subjectivity
            
            subjectivity.append(subjectivity)

            return  pd.DataFrame(polarity), pd.DataFrame(subjectivity)


    def find_created_time(self)->list:
       
        tweets = pd.DataFrame(self.tweets_list)

        return tweets['created_at']

    def find_source(self)->list:
       
        tweets = pd.DataFrame(self.tweets_list)


        return tweets['source']

    def find_screen_name(self)->list:
        
        tweets = pd.DataFrame(self.tweets_list)
        
        screen_name=tweets["user"].get("screen_name")
                
        return screen_name


    def find_followers_count(self)->list:
        
        tweets = pd.DataFrame(self.tweets_list)
        
        followers_count=tweets["user"].get("followers_count")

        return followers_count

    def find_friends_count(self)->list:
        
        tweets = pd.DataFrame(self.tweets_list)
        
        friends_count=tweets["user"].get("friends_count")

        return friends_count

    def is_sensitive(self)->list:
                
        tweets = pd.DataFrame(self.tweets_list)

        try:
            is_sensitive = tweets["possibly_sensitive"]
        except KeyError:
            is_sensitive = None

        return pd.DataFrame(is_sensitive)

    def find_favourite_count(self)->list:
        
        tweets = pd.DataFrame(self.tweets_list)
        
        favourites_count=tweets["user"].get("favourites_count")

        return favourites_count

    
    def find_retweet_count(self)->list:
        
        tweets = pd.DataFrame(self.tweets_list )
        
        return tweets['retweet_count']

    def find_hashtags(self)->list:
         
        tweets = pd.DataFrame(self.tweets_list)
        
        return tweets['entities'].get("hashtags")

    def find_mentions(self)->list:
         
        tweets = pd.DataFrame(self.tweets_list)
        
        return tweets['entities'].get("user_mentions")

    def find_location(self)->list:
        try:
            tweets = pd.DataFrame(self.tweets_list)
        except TypeError:
            location = ''

        return tweets['user'].get("location")
    
    def find_lang(self)->list:
    
        tweets = pd.DataFrame(self.tweets_list)
        
        return tweets["lang"]
        
    def get_tweet_df(self, save=False)->pd.DataFrame:
        """required column to be generated you should be creative and add more features"""
        
        columns = ['created_at', 'source', 'original_text','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
            'original_author', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place']
        
        created_at = self.find_created_time()
        source = self.find_source()
        text = self.find_full_text()
        polarity, subjectivity = self.find_sentiments()
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
        data = zip(created_at, source, text, polarity, subjectivity, lang, fav_count, retweet_count, screen_name, follower_count, friends_count, sensitivity, hashtags, mentions, location)
        df = pd.DataFrame(data=data, columns=columns)
        if save:
            df.to_csv('processed_tweet_data.csv', index=False)
            print('File Successfully Saved.!!!')
        
        return df

                
if __name__ == "__main__":
    # required column to be generated you should be creative and add more features
    columns = ['created_at', 'source', 'text','clean_text', 'sentiment','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
    'original_author', 'screen_count', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place', 'place_coord_boundaries']
    tweet_list = read_json("data/Economic_Twitter_Data.json")
    #tweet_list =  zip(columns,tweet_list) 

    tweet = TweetDfExractor(tweet_list)
    tweet_df = tweet.get_tweet_df() 

    # use all defined functions to generate a dataframe with the specified columns above

    
