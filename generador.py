import os
import json
import shutil
import bz2
import networkx as nx
from collections import defaultdict
import collections
import sys
import itertools
import argparse
import argparse


# Generates a function that gets the following parameters once started or running the program:
# a.	-d <relative> :  (default value: data)
# b.	-fi <date inicial> : date (dd-mm-yy)
# c.	-ff <date final> : date (dd-mm-yy)
# d.	-h <nombre de archivo>:Nombre de 
# archivo de texto en el 
# que se encuentra los hashtags por 
# los cuales se filtrarán los tweets,
# uno en cada líne
# a.	-grt: (could be or not is a boolean if it exists it is true)
# b.	-jrt: (could be or not is a boolean if it exists it is true)
# c.	-gm: (could be or not is a boolean if it exists it is true)
# d.    -jm: (could be or not is a boolean if it exists it is true)
# e.	-gcrt: (could be or not is a boolean if it exists it is true)
# f.    -jcrt: (could be or not is a boolean if it exists it is true)

def get_parameters(argv):
    parser = argparse.ArgumentParser(description='Process some parameters.', add_help=False)
    parser.add_argument('-d', '--directory', type=str, default='data', help='Relative directory (default: data)')
    parser.add_argument('-fi', '--start-date', type=str, help='Initial date (dd-mm-yy)')
    parser.add_argument('-ff', '--end-date', type=str, help='Final date (dd-mm-yy)')
    parser.add_argument('-h', '--hashtags_file', type=str, help='File with hashtags, one per line')
    parser.add_argument('-grt', '--graph_retweets', action='store_true', help='Graph retweets')
    parser.add_argument('-jrt', '--json_retweets', action='store_true', help='JSON retweets')
    parser.add_argument('-gm', '--graph_mentions', action='store_true', help='Graph mentions')
    parser.add_argument('-jm', '--json_mentions', action='store_true', help='JSON mentions')
    parser.add_argument('-gcrt', '--graph_corretweets', action='store_true', help='Graph corretweets')
    parser.add_argument('-jcrt', '--json_corretweets', action='store_true', help='JSON corretweets')
    args = parser.parse_args(argv)
    return vars(args)

# Generates a function
# that takes as a parameter the name of a file of a json.bz2 type
# and returns a list of dictionaries taken from each line of the file
def read_json_bz2(filename, restriction="none"):
    tweets = []
    with bz2.open(filename, "rt", encoding="utf-8") as bzinput:
        if restriction == "rts":
            tweets = [json.loads(line) for line in bzinput if 
                      "retweeted_status" in json.loads(line)]
        elif restriction == "mtns":
           tweets = [json.loads(line) for line in bzinput if 
                     "entities" in json.loads(line) and "user_mentions" 
                     in json.loads(line)["entities"]]
        else:
            tweets = [json.loads(line) for line in bzinput]
    return tweets

def initialize_retweets_dict():
    return defaultdict(lambda: {"receivedRetweets": 0, "tweets": {}})

def process_retweet(tweet, retweets_dict):
    retweeter_username = tweet["user"]["screen_name"]
    retweeted_status = tweet["retweeted_status"]

    original_tweet_id = "tweetId: " + str(retweeted_status["id"])
    original_tweet_username = retweeted_status["user"]["screen_name"]

    # Increase the counter of retweets received to the original author
    retweets_dict[original_tweet_username]["receivedRetweets"] += 1

    # Add info about the retweet to the original tweet
    if original_tweet_id not in retweets_dict[original_tweet_username]["tweets"]:
        retweets_dict[original_tweet_username]["tweets"][original_tweet_id] = {"retweetedBy": []}

    retweets_dict[original_tweet_username]["tweets"][original_tweet_id]["retweetedBy"].append(retweeter_username)

def convert_dict_to_list(retweets_dict):
    return [{"username": username, **data} for username, data in retweets_dict.items()]

def export_to_json(result_dict):
    with open("rt.json", "w") as json_file:
        json.dump(result_dict, json_file, indent=2)

def process_retweets(json_list):
    retweets_dict = initialize_retweets_dict()

    for tweet in json_list:
        process_retweet(tweet, retweets_dict)

    retweets_list = convert_dict_to_list(retweets_dict)

    result_dict = {"retweets": retweets_list}

    export_to_json(result_dict)

    return result_dict

# Main function
def main(args):
    path = os.getcwd()
    print(path)
    print(args)
    print(get_parameters(args))
    # Read the json from the relative directory
    tweets_list = read_json_bz2(os.path.join(path, "30.json.bz2"), restriction="rts")
    print(tweets_list[0])
    print(len(tweets_list))
    print(type(process_retweets(tweets_list)))
    
# If name is main, then the program is running directly
if __name__ == '__main__':
    main(sys.argv[1:])