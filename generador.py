import os
import json
import shutil
import bz2
import networkx as nx
from collections import defaultdict
from datetime import datetime, timedelta
import sys
import argparse
import numpy as np


# Generates a function that gets the following parameters once started or running the program:
# a.	-d <relative> :  (default value: data)
# b.	-fi <date inicial> : date (dd-mm-yy)
# c.	-ff <date final> : date (dd-mm-yy)
# d.	-h <nombre de archivo>:Nombre de
# archivo de texto en el
# que se encuentra los hashtags por
# los cuales se filtrarán los tweets,
# uno en cada línea
# e.	-grt: (could be or not is a boolean if it exists it is true)
# f.	-jrt: (could be or not is a boolean if it exists it is true)
# g.	-gm: (could be or not is a boolean if it exists it is true)
# h.    -jm: (could be or not is a boolean if it exists it is true)
# i.	-gcrt: (could be or not is a boolean if it exists it is true)
# j.    -jcrt: (could be or not is a boolean if it exists it is true)
def get_parameters(argv):
    parser = argparse.ArgumentParser(
        description="Process some parameters.", add_help=False
    )
    parser.add_argument(
        "-d",
        "--directory",
        type=str,
        default="data",
        help="Relative directory (default: data)",
    )
    parser.add_argument("-fi", "--start-date", type=str, help="Initial date (dd-mm-yy)")
    parser.add_argument("-ff", "--end-date", type=str, help="Final date (dd-mm-yy)")
    parser.add_argument(
        "-h", "--hashtags_file", type=str, help="File with hashtags, one per line"
    )
    parser.add_argument(
        "-grt", "--graph_retweets", action="store_true", help="Graph retweets"
    )
    parser.add_argument(
        "-jrt", "--json_retweets", action="store_true", help="JSON retweets"
    )
    parser.add_argument(
        "-gm", "--graph_mentions", action="store_true", help="Graph mentions"
    )
    parser.add_argument(
        "-jm", "--json_mentions", action="store_true", help="JSON mentions"
    )
    parser.add_argument(
        "-gcrt", "--graph_corretweets", action="store_true", help="Graph corretweets"
    )
    parser.add_argument(
        "-jcrt", "--json_corretweets", action="store_true", help="JSON corretweets"
    )
    args = parser.parse_args(argv)
    return vars(args)

# Create a function named read_json_files_bz2 that takes as a parameter a directory
# and returns a list of dictionaries taken from each line of the files
# of each json.bz2 file in the directory finally concatenate all the lists
def read_json_files_bz2(directory, restriction="none"):
    # Get the list of files in the directory
    list_of_files = os.listdir(directory)
    # Create a list of lists of dictionaries
    list_of_lists = []
    # Iterate over the list of files
    # # Read the json.bz2 file
    list_of_lists = [
        read_json_bz2(os.path.join(directory, file), restriction)
        for file in list_of_files
    ]
    # Concatenate the lists
    return concatenate_lists(list_of_lists)


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
        retweets_dict[original_tweet_username]["tweets"][original_tweet_id] = {
            "retweetedBy": []
        }

    retweets_dict[original_tweet_username]["tweets"][original_tweet_id][
        "retweetedBy"
    ].append(retweeter_username)


def convert_dict_to_list(retweets_dict):
    return [{"username": username, **data} for username, data in retweets_dict.items()]


def export_to_json(result_dict, write=False):
    if write:
        # Write to a new JSON file
        output_filename = "rt.json"
        with open(output_filename, "w") as json_file:
            json.dump(result_dict, json_file, indent=2)
    return result_dict


def process_retweets(json_list, write=False):
    retweets_dict = initialize_retweets_dict()

    for tweet in json_list:
        process_retweet(tweet, retweets_dict)

    retweets_list = convert_dict_to_list(retweets_dict)
    # Sort the list by the number of retweets received
    retweets_list = sorted(
        retweets_list, key=lambda x: x["receivedRetweets"], reverse=True
    )

    result_dict = {"retweets": retweets_list}
    if write:
        export_to_json(result_dict, write=write)
    return result_dict


def process_mentions(json_list, write=False):
    mentions_list = []

    for tweet in json_list:
        user_mentions = tweet.get("entities", {}).get("user_mentions", [])
        tweet_id = str(tweet.get("id", ""))

        for mention in user_mentions:
            username = mention.get("screen_name", "")
            found_mention = next(
                (m for m in mentions_list if m["username"] == username), None
            )

            if found_mention is None:
                mention_entry = {
                    "username": username,
                    "receivedMentions": 1,
                    "mentions": [
                        {
                            "mentionBy": tweet["user"]["screen_name"],
                            "tweets": [tweet_id],
                        }
                    ],
                }
                mentions_list.append(mention_entry)
            else:
                found_tweet = next(
                    (
                        t
                        for t in found_mention["mentions"]
                        if t["mentionBy"] == tweet["user"]["screen_name"]
                        and tweet_id in t["tweets"]
                    ),
                    None,
                )
                if found_tweet is None:
                    found_mention["receivedMentions"] += 1
                    found_mention["mentions"].append(
                        {
                            "mentionBy": tweet["user"]["screen_name"],
                            "tweets": [tweet_id],
                        }
                    )
    # Parse the list to a json
    mentions_list = sorted(
        mentions_list, key=lambda x: x["receivedMentions"], reverse=True
    )
    # delete all the mentions for null
    mentions_list = [x for x in mentions_list if x["username"] != "null"]
    result = {"mentions": mentions_list}
    save_to_json(result, "mención.json", write=write)
    return result


def save_to_json(data, filename, write=False):
    if write:
        with open(filename, "w") as json_file:
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
        "retweeters": common_retweeters,
    }


def process_corretweets(tweet_list, write=False):
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

    dictionary_of_corr = {"coretweets": coretweets}
    # Write to a new JSON file named crrtw.json if the write parameter is true
    if write:
        output_filename = "crrtw.json"
        with open(output_filename, "w") as json_file:
            json.dump(dictionary_of_corr, json_file, indent=2)
    return dictionary_of_corr


# Create a function named mentions_graph that takes as a parameter a list of tweets
# and returns a graph with the mentions, also create the graph in gexf format
def mentions_graph(mentions_list):
    # Create a graph
    graph = nx.Graph()
    # Add the nodes
    for mention in mentions_list:
        graph.add_node(
            mention["username"], receivedMentions=mention["receivedMentions"]
        )
    # Add the edges
    for mention in mentions_list:
        for mention_data in mention["mentions"]:
            for tweet in mention_data["tweets"]:
                graph.add_edge(
                    mention["username"], mention_data["mentionBy"], tweetId=tweet
                )
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
        graph.add_node(
            retweet["username"], receivedRetweets=retweet["receivedRetweets"]
        )
    # Add the edges
    for retweet in retweets_list:
        for tweet in retweet["tweets"]:
            for retweeter in retweet["tweets"][tweet]["retweetedBy"]:
                graph.add_edge(retweet["username"], retweeter, tweetId=tweet)
    # Save the graph in gexf format
    nx.write_gexf(graph, "rt.gexf")
    # Return the graph
    return graph


# Create a function named corretweets_graph that takes as a parameter a list of tweets
# and returns a graph with the corretweets, also create the graph in gexf format
# Take in account that the more corretweets more weight the edge has
def corretweets_graph(coretweets_list):
    # Create a graph
    graph = nx.Graph()
    # Add the nodes
    for coretweet in coretweets_list:
        graph.add_node(coretweet["authors"]["u1"])
        graph.add_node(coretweet["authors"]["u2"])
    # Add the edges
    for coretweet in coretweets_list:
        graph.add_edge(
            coretweet["authors"]["u1"],
            coretweet["authors"]["u2"],
            weight=coretweet["totalCoretweets"],
        )
    # Save the graph in gexf format
    nx.write_gexf(graph, "crrtw.gexf")
    # Return the graph
    return graph


def get_directories_with_json_bz2(base_directory):
    return [
        os.path.join(current_folder, file)
        for current_folder, _, files in os.walk(base_directory)
        for file in files
        if file.endswith(".json.bz2")
    ]


# Generates a function
# that takes as a parameter the name of a file of a json.bz2 type
# and returns a list of dictionaries taken from each line of the file
def read_json_bz2(filename, restriction=None, start_date=None, end_date=None, hashtags=None):
    tweets = []
    with bz2.open(filename, "rt", encoding="utf-8") as bzinput:
        if restriction == "rts":
            tweets = [
                json.loads(line)
                for line in bzinput
                if "retweeted_status" in json.loads(line)
            ]
        elif restriction == "mtns":
            tweets = [
                json.loads(line)
                for line in bzinput
                if "entities" in json.loads(line)
                and "user_mentions" in json.loads(line)["entities"]
                and not ("retweeted_status" in json.loads(line))
            ]
        else:
            tweets = [json.loads(line) for line in bzinput]
        tweets_by_date = filter_by_date(tweets, start_date, end_date)
    return tweets

# Create a function that receives a list of dictionaries
# and a start date and an end date with the format "dd-mm-yy" (It could be None)
# and be sure that the date of the tweet is between the start date and the end date
# the date of the tweet is in the key "created_at" of the dictionary
# and it is in the format "Wed Jun 25 04:08:58 +0000 2014"
# if the start date is None then just ignore it
# if the end date is None then just ignore it
def filter_by_date(tweets_list, start_date=None, end_date=None):
    if start_date is not None:
        start_date = datetime.strptime(start_date, "%d-%m-%y")
    if end_date is not None:
        end_date = datetime.strptime(end_date, "%d-%m-%y")
    return [
        tweet
        for tweet in tweets_list
        if (
            (start_date is None or datetime.strptime(tweet["created_at"], "%a %b %d %H:%M:%S %z %Y") >= start_date)
            and (end_date is None or datetime.strptime(tweet["created_at"], "%a %b %d %H:%M:%S %z %Y") <= end_date)
        )
    ]


# Create a function named concatenate_lists that takes as a parameter a list of lists
# and returns a list with all the elements of the lists, use sum(list, []) to concatenate
def concatenate_lists(list_of_lists):
    return sum(list_of_lists, [])



# Main function
def main(args):
    path = os.getcwd()
    print(path)
    print(args)
    print(type(get_parameters(args)))
    print(get_directories_with_json_bz2(path + "/testing"))

    # Read the json from the relative directory
    # tweets_list = read_json_files_bz2_date_range(path+"/testing", "01-01-16", "08-02-16", restriction="rts")
    print(tweets_list[0])
    print(len(tweets_list))
    dictionary = process_retweets(tweets_list, write=True)
    retweets_graph(dictionary["retweets"])


# If name is main, then the program is running directly
if __name__ == "__main__":
    main(sys.argv[1:])
