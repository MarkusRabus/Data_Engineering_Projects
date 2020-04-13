# Apache Cassandra Database


## Introduction

Sparkify wants to analyze the data on songs and user activity on their new 
music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. 
Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of 
CSV files on user activity on the app.

In this project I modelled the data with Apache Cassandra and created queries on song play data to analyze data collected by Sparkify's music streaming app. 

## Data Analysis

I have created tables optimized to answer the following three questions of the data:
1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own

## ETL pipeline
The ETL pipeline is in implemented in the `Project_1B_Project_Template.ipynb` notebook.
The CSV files with the data collected by the music streaming up can be found in the `event_data` folder

