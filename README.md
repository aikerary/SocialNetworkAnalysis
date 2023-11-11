# Project README - Tweet Database Processor

## Introduction

This project involves processing a database of tweets based on specified parameters. The main goal is to generate various outputs, including graphs and JSON files, to analyze and understand the relationships and interactions within the tweet data.

## Project Structure

The main deliverable for this project is a Python script named `generador.py`. This script will accept specific input parameters to customize the processing of the tweet database.

### Input Parameters

1. **-d <path relativo>:** Relative directory where the tweets are located. The script should traverse the entire folder structure to find tweets, including those in subdirectories.

2. **-fi <fecha inicial>:** Start date (dd-mm-aa) from which tweets should be considered. Ignore this restriction if the parameter is not present.

3. **-ff <fecha final>:** End date (dd-mm-aa) until which tweets should be considered. Ignore this restriction if the parameter is not present.

4. **-h <nombre de archivo>:** Text file containing hashtags for filtering tweets. Each line should contain a single hashtag. Ignore this restriction if the parameter is not present.

### Output Parameters

The script will generate different outputs based on the following parameters:

1. **-grt:** Retweet graph (rt.gexf), including all nodes.

2. **-jrt:** JSON of retweets (rt.json) presenting authors with their tweets (id) that were retweeted. Also includes a list of users who retweeted each tweet. The JSON should be sorted from highest to lowest by the total number of retweets to the user.

3. **-gm:** Mention graph (mención.gexf).

4. **-jm:** JSON of mentions (mención.json) presenting authors who were mentioned and a list of users who mentioned them with the tweet ID. The JSON should be sorted from highest to lowest by the total number of mentions to the user.

5. **-gcrt:** Co-retweet graph (corrtw.gexf) - The Co-Retweeted Network and its Applications for Measuring the Perceived Political Polarization.

6. **-jcrt:** JSON of co-retweets (corrtw.json) presenting each pair of authors and a list of authors who retweeted them.

### Additional Notes

- The tweets are in JSON format within a compressed file.

- Each tweet follows a specific structure.

- The script will use the `networkx` library to create graphs from the edge list, which can be visualized using Gephi.

- Before completion, any temporary files must be deleted.

- The only output should be the total execution time in seconds.

## Repository Guidelines

Please ensure that the project is delivered in a repository with the surnames of the group members. Do not include the tweets in the repository.

## Execution Example

```bash
python generador.py -d data -fi 01-01-22 -ff 31-12-22 -h hashtags.txt -grt -jrt -gm -jm -gcrt -jcrt
```

This example command processes the tweet database in the 'data' directory, considering tweets from January 1, 2022, to December 31, 2022, with hashtag filtering. It generates retweet and mention graphs, corresponding JSON files, and co-retweet graph and JSON. The execution time will be printed as the only output.