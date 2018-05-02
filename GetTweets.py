
import pickle
import tweepy
import os
import copy
import time
import glob
import json
TWITTER_CONSUMER_KEY = r'zmQRQkwK05yASvjroOQ2OWV05'
TWITTER_CONSUMER_SECRET = r'HbE5mk8rtUpWlgf33q4hi8ouN8crH0BpEGPQVuXQxHpUih9v8y'
TWITTER_ACCESS_TOKEN = r'965958691655647232-lsBm1mVJ1dl7c4npuKSk7E0D827u8BQ'
TWITTER_ACCESS_TOKEN_SECRET = r'KqZFlLLkNeVKzwehQUPaVH0fEXxbPgxn6JD9sACBEo5iR'

DATA_FORMAT = {'user_info':None, 'following':None, 'tweets':None}
DATA = {}
auth = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY,
                            TWITTER_CONSUMER_SECRET)

auth.set_access_token(TWITTER_ACCESS_TOKEN,
                        TWITTER_ACCESS_TOKEN_SECRET)

api = tweepy.API(auth,wait_on_rate_limit=True)


topics = ['911WasAnInsideJob', 'JFKFiles', 'TransformationTuesday', 'UFOs']



def get_user_tweets(user_id):
    c = tweepy.Cursor(api.user_timeline, id=user_id, include_rts=True, tweet_mode='extended').items()
    tweets = []
    print('Retrieving tweets from user: ' + str(user_id))
    while True:
        try:
            item = c.next()
            tweets.append(item)
        except tweepy.TweepError:
            # hit rate limit, sleep for 15 minutes
            print('Rate limited. Sleeping for 15 minutes.')
            time.sleep(15 * 60 + 15)
            continue
        except StopIteration:
            print('Done!')
            break
    return [tweet.retweeted_status.full_text 
            if 'retweeted_status' 
            in dir(tweet) 
            else tweet.full_text 
            for tweet 
            in tweets]

def get_search_tweets(search_term, min_friends_count=0, min_followers_count=0, n_items=None):
    c = tweepy.Cursor(api.search, q=search_term).items(n_items)
    tweets = []
    print('Retrieving tweets from search: ' + str(search_term))
    while True:
        try:
            item = c.next()
            if item.user.followers_count >= min_followers_count and item.user.friends_count >= min_friends_count:
                tweets.append(item)
        except tweepy.TweepError:
            print('Rate limited. Sleeping for 15 minutes.')
            time.sleep(15 * 60 + 15)
            continue
        except StopIteration:
            print('Done!')
            break
    return tweets

    
if __name__ == '__main__':
    
    data_org = {}
    done = []
    if os.path.exists('data_org.pkl'):
        with open('data_org.pkl', 'rb') as f:
            data_org = pickle.load(f)
    print('Building query structure...')
    for topic in topics:
        if topic in data_org.keys():
            searched = glob.glob(os.path.join(data_org[topic]['out_dir'], '*.json'))
            data_org[topic]['index'] = max(len(searched)-1, 0)
            done.extend([int(os.path.split(json_file)[1].replace('.json','')) for json_file in searched])
            continue
        data_org[topic] = {}
        data_org[topic]['out_dir'] = os.path.join(os.curdir, 'tweets_' + topic)
        data_org[topic]['search_count'] = 0
        data_org[topic]['index'] = 0
        if not os.path.exists(data_org[topic]['out_dir']):
            os.makedirs(data_org[topic]['out_dir'])
        data_org[topic]['search_results'] = get_search_tweets('#'+topic, 5, 5, 200)
        data_org[topic]['search_count'] = len(data_org[topic]['search_results'])

    with open('data_org.pkl', 'wb') as f:
        pickle.dump(data_org, f)
    print('Done!')

    print('Beginning user tweet crawl...')
    not_finished = True
    while not_finished:
        not_finished = False
        for topic in topics:
            if data_org[topic]['search_count'] - data_org[topic]['index'] == 0:
                continue
            not_finished = True
            user_id = data_org[topic]['search_results'][data_org[topic]['index']].user.id
            if user_id in done:
                data_org[topic]['index'] += 1
                continue
            tweets = get_user_tweets(user_id)
            userfname = os.path.join(data_org[topic]['out_dir'], str(user_id) + '.json')
            user_data = {'user': user_id, 'tweets':tweets}
            with open(userfname, 'w') as f:
                f.write(json.dumps(user_data, indent=1))
                data_org[topic]['index'] += 1
                done.append(user_id)
            if len(glob.glob(os.path.join(data_org[topic]['out_dir'], '*.json'))) >= 25:
                not_finished=False


    print('Done!')
