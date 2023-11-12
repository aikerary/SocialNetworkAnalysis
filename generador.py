import os
import json
import shutil
import bz2
import getopt
import networkx as nx
from collections import defaultdict
import sys
import itertools
import argparse
import argparse
path = os.getcwd()

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
    parser.add_argument('-fi', '--fecha_inicial', type=str, help='Initial date (dd-mm-yy)')
    parser.add_argument('-ff', '--fecha_final', type=str, help='Final date (dd-mm-yy)')
    parser.add_argument('-h', '--hashtags_file', type=str, help='File with hashtags, one per line')
    parser.add_argument('-grt', '--Graph_retweets', action='store_true', help='Graph retweets')
    parser.add_argument('-jrt', '--JSON_retweets', action='store_true', help='JSON retweets')
    parser.add_argument('-gm', '--Graph_mentions', action='store_true', help='Graph mentions')
    parser.add_argument('-jm', '--JSON_mentions', action='store_true', help='JSON mentions')
    parser.add_argument('-gcrt', '--Graph_citations', action='store_true', help='Graph citations')
    parser.add_argument('-jcrt', '--JSON_citations', action='store_true', help='JSON citations')
    args = parser.parse_args(argv)
    return vars(args)
    


print(get_parameters(sys.argv[1:]))