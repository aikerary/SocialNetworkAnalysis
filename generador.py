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
def read_json_bz2(filename):
    tweets = []
    with bz2.open(filename, "rt", encoding="utf-8") as bzinput:
        tweets=[json.loads(line.strip()) for line in bzinput]
    return tweets

# Main function
def main(args):
    path = os.getcwd()
    print(path)
    print(args)
    print(get_parameters(args))
    # Read the json from the relative directory
    tweets = read_json_bz2(os.path.join(path, "30.json.bz2"))
    print(tweets[0])
    
# If name is main, then the program is running directly
if __name__ == '__main__':
    main(sys.argv[1:])