import os
import json
import shutil
import bz2
import networkx as nx
from collections import Counter, defaultdict
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
                     in json.loads(line)["entities"] and not("retweeted_status" in json.loads(line))]
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
    # Sort the list by the number of retweets received
    retweets_list = sorted(retweets_list, key=lambda x: x["receivedRetweets"], reverse=True)

    result_dict = {"retweets": retweets_list}

    export_to_json(result_dict)

    return result_dict


def process_mentions(json_list):
    mentions_dict = extract_mentions(json_list)
    mentions_list = list(mentions_dict.values())
    # Sort the list by the number of mentions received
    mentions_list = sorted(mentions_list, key=lambda x: x["receivedMentions"], reverse=True)
    result = {"mentions": mentions_list}
    save_to_json(result, 'mención.json')
    return result

def extract_mentions(json_list):
    mentions_dict = {}

    for tweet in json_list:
        user_mentions = tweet.get("entities", {}).get("user_mentions", [])
        tweet_id = str(tweet.get("id", ""))

        for mention in user_mentions:
            username = mention.get("screen_name", "")
            if username not in mentions_dict:
                mentions_dict[username] = {
                    "username": username,
                    "receivedMentions": 0,
                    "mentions": []
                }

            mentions_dict[username]["receivedMentions"] += 1
            mentions_dict[username]["mentions"].append({
                "mentionBy": tweet["user"]["screen_name"],
                "tweets": [tweet_id]
            })

    return mentions_dict

def save_to_json(data, filename):
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=2)

def extract_retweets(tweet_list):
    retweets_by_author = defaultdict(list)
    
    for tweet in tweet_list:
        screen_name = tweet["user"]["screen_name"]
        retweeted_status = tweet.get("retweeted_status")
        
        if retweeted_status:
            original_author_screen_name = retweeted_status["user"]["screen_name"]
            retweets_by_author[original_author_screen_name].append(screen_name)
    
    return retweets_by_author

def find_common_retweeters(retweeters1, retweeters2):
    return list(set(retweeters1) & set(retweeters2))

def generate_coretweet(author1, author2, common_retweeters):
    pair = tuple(sorted([author1, author2]))
    return {
        "authors": {"u1": pair[0], "u2": pair[1]},
        "totalCoretweets": len(common_retweeters),
        "retweeters": common_retweeters
    }

def find_coretweets(tweet_list):
    coretweets = []
    retweets_by_author = extract_retweets(tweet_list)
    seen_pairs = set()

    for author1, retweeters1 in retweets_by_author.items():
        for author2, retweeters2 in retweets_by_author.items():
            if author1 != author2:
                common_retweeters = find_common_retweeters(retweeters1, retweeters2)
                if common_retweeters:
                    coretweet = generate_coretweet(author1, author2, common_retweeters)
                    if tuple(sorted([author1, author2])) not in seen_pairs:
                        coretweets.append(coretweet)
                        seen_pairs.add(tuple(sorted([author1, author2])))
    # Sort the coretweets by the number of retweeters
    coretweets = sorted(coretweets, key=lambda x: x["totalCoretweets"], reverse=True)
    return {"coretweets": coretweets}

def write_to_json(data):
    with open("crrtw.json", "w") as outfile:
        json.dump(data, outfile, indent=2)

# Create a function named mentions_graph that takes as a parameter a list of tweets
# and returns a graph with the mentions, also create the graph in gexf format
def mentions_graph(mentions_list):
    # Create a graph
    graph = nx.Graph()
    # Add the nodes
    for mention in mentions_list:
        graph.add_node(mention["username"], receivedMentions=mention["receivedMentions"])
    # Add the edges
    for mention in mentions_list:
        for mention_data in mention["mentions"]:
            for tweet in mention_data["tweets"]:
                graph.add_edge(mention["username"], mention_data["mentionBy"], tweetId=tweet)
    # Save the graph in gexf format
    nx.write_gexf(graph, "mención.gexf")
    # Return the graph
    return graph

# Create a function named retweets_graph that takes as a parameter a list of tweets
# and returns a graph with the retweets, also create the graph in gexf format
def retweets_graph(retweets_list):
    # Create a graph
    graph = nx.Graph()
    # Add the nodes
    for retweet in retweets_list:
        graph.add_node(retweet["username"], receivedRetweets=retweet["receivedRetweets"])
    # Add the edges
    for retweet in retweets_list:
        for tweet in retweet["tweets"]:
            for retweeter in retweet["tweets"][tweet]["retweetedBy"]:
                graph.add_edge(retweet["username"], retweeter, tweetId=tweet)
    # Save the graph in gexf format
    nx.write_gexf(graph, "rt.gexf")
    # Return the graph
    return graph

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
    dictionary = process_retweets(tweets_list)
    retweets_graph(dictionary["retweets"])
    
# If name is main, then the program is running directly
if __name__ == '__main__':
    main(sys.argv[1:])