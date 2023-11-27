import os
import json
import shutil
import bz2
import networkx as nx
from collections import defaultdict
from datetime import datetime, timedelta
import sys
import argparse
from datetime import datetime, timezone

# Import things to measure the time
import time
from mpi4py import MPI


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
def read_json_bz2(
    filename, restriction=None, start_date=None, end_date=None, hashtags=None
):
    tweets = []
    with bz2.open(filename, "rt", encoding="utf-8") as bzinput:
        parsed_lines = (json.loads(line) for line in bzinput)
        hashtags_set = set(hashtags) if hashtags else set()
        
        if restriction == "rts":
            tweets = [
                line
                for line in parsed_lines
                if "entities" in line
                and "retweeted_status" in line
                and filter_by_date(line, start_date, end_date)
                and filter_by_hashtags(line, hashtags_set)
            ]
        elif restriction == "mtns":
            tweets = [
                line
                for line in parsed_lines
                if "entities" in line
                and "user_mentions" in line["entities"]
                and "retweeted_status" not in line
                and filter_by_date(line, start_date, end_date)
                and filter_by_hashtags(line, hashtags_set)
            ]
        else:
            tweets = [
                line
                for line in parsed_lines
                if "entities" in line
                and filter_by_date(line, start_date, end_date)
                and filter_by_hashtags(line, hashtags_set)
            ]
    return tweets


# Create a function that receives a single tweet
# and a start date and an end date with the format "dd-mm-yy" (It could be None)
# and be sure that the date of the tweet is between the start date and the end date
# the date of the tweet is in the key "created_at" of the dictionary
# and it is in the format "Wed Jun 25 04:08:58 +0000 2014"
# if the start date is None then just ignore it


def filter_by_date(tweet, start_date=None, end_date=None):
    if start_date is None and end_date is None:
        return True
    elif start_date is None:
        return datetime.strptime(
            tweet["created_at"], "%a %b %d %H:%M:%S %z %Y"
        ) <= datetime.strptime(end_date, "%d-%m-%y").replace(tzinfo=timezone.utc)
    elif end_date is None:
        return datetime.strptime(
            tweet["created_at"], "%a %b %d %H:%M:%S %z %Y"
        ) >= datetime.strptime(start_date, "%d-%m-%y").replace(tzinfo=timezone.utc)
    else:
        return datetime.strptime(
            tweet["created_at"], "%a %b %d %H:%M:%S %z %Y"
        ) >= datetime.strptime(start_date, "%d-%m-%y").replace(
            tzinfo=timezone.utc
        ) and datetime.strptime(
            tweet["created_at"], "%a %b %d %H:%M:%S %z %Y"
        ) <= datetime.strptime(
            end_date, "%d-%m-%y"
        ).replace(
            tzinfo=timezone.utc
        )
    return hashtags


# Create a function that receives a single tweet and a lsit of hashtags (it could be empty or be None)
# and be sure that the tweet has at least one of the hashtags in the list
# the hashtags are a list in entities -> hashtags
# if the list of hashtags is None then just ignore it
# if the list of hashtags is empty then just ignore it
# also verify if it does not have entities field
def filter_by_hashtags(tweet, hashtags=None):
    if hashtags is None or hashtags == []:
        return True
    else:
        return any(
            hashtag["text"].lower() in hashtags
            for hashtag in tweet["entities"]["hashtags"]
        )


# Create a function named concatenate_lists that takes as a parameter a list of lists
# and returns a list with all the elements of the lists, use sum(list, []) to concatenate
def concatenate_lists(list_of_lists):
    return sum(list_of_lists, [])


# Create a function that receives a base directory, a restriction (rts, mtns or none),
# an start date, an end date and a list of hashtags (it could be empty or be None)
# and returns a list of tweets that are in all the possible json.bz2 files in all the subdirectories
# of the base directory

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

def read_json_files(
    base_directory, start_date=None, end_date=None, restriction=None, hashtags=None
):
    # Get the list of directories with json.bz2 files
    list_of_directories = get_directories_with_json_bz2(base_directory)
    # Divide the directories among processes
    chunk_size = len(list_of_directories) // size
    start_index = rank * chunk_size
    end_index = (rank + 1) * chunk_size if rank < size - 1 else len(list_of_directories)
    local_directories = list_of_directories[start_index:end_index]
    
    # Read the json.bz2 files for local directories
    local_lists = [
        read_json_bz2(
            directory,
            restriction,
            start_date,
            end_date,
            hashtags,
        )
        for directory in local_directories
    ]
    
    # Gather the local lists from all processes
    all_lists = comm.gather(local_lists, root=0)
    
    if rank == 0:
        # Concatenate the lists
        concatenated_lists = concatenate_lists(sum(all_lists, []))
        return concatenated_lists
    else:
        return None

def filter_if_retweet(tweet):
    if "retweeted_status" in tweet:
        return True
    else:
        return False
    
def filter_if_mention(tweet):
    if "user_mentions" in tweet["entities"] and not ("retweeted_status" in tweet):
        return True
    else:
        return False

# Main function
def main(args):
    path = os.getcwd()
    # Save the arguments
    arguments = get_parameters(args)
    # Save the arguments in their corresponding variables
    directory = arguments["directory"]
    start_date = arguments["start_date"]
    end_date = arguments["end_date"]
    hashtags_file = arguments["hashtags_file"]
    graph_retweets = arguments["graph_retweets"]
    json_retweets = arguments["json_retweets"]
    graph_mentions = arguments["graph_mentions"]
    json_mentions = arguments["json_mentions"]
    graph_corretweets = arguments["graph_corretweets"]
    json_corretweets = arguments["json_corretweets"]
    # Start to measure the time
    start_time = time.time()
    if not (directory is None):
        # the path is now the relative path to the directory
        path = os.path.join(path, directory)
    if hashtags_file is None:
        hashtags = []
    else:
        hashtags = [line.rstrip("\n") for line in open(hashtags_file)]
    if graph_mentions or json_mentions or graph_retweets or json_retweets or graph_corretweets or json_corretweets:
        tweets_list = read_json_files(
            path, start_date, end_date, restriction="none", hashtags=hashtags
        )
    if graph_retweets or json_retweets:
        # Filter by retweets
        tweets_list_rts = list(filter(filter_if_retweet, tweets_list))
        # Process the retweets
        dictionary = process_retweets(tweets_list_rts, write=json_retweets)
        if graph_retweets:
            # Create the graph
            retweets_graph(dictionary["retweets"])
    if graph_mentions or json_mentions:
        # Filter by mentions
        tweets_list_mtn = list(filter(filter_if_mention, tweets_list))
        # Process the mentions
        dictionary = process_mentions(tweets_list_mtn, write=json_mentions)
        if graph_mentions:
            # Create the graph
            mentions_graph(dictionary["mentions"])
    if graph_corretweets or json_corretweets:
        # Filter by retweets
        tweets_list_crt = list(filter(filter_if_retweet, tweets_list))
        # Process the corretweets
        dictionary = process_corretweets(tweets_list_crt, write=json_corretweets)
        if graph_corretweets:
            # Create the graph
            corretweets_graph(dictionary["coretweets"])
    # Print the time it took to run the program
    print(time.time() - start_time)


# If name is main, then the program is running directly
if __name__ == "__main__":
    main(sys.argv[1:])