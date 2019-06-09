#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[1]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from prettytable import PrettyTable


# #### Creating list of filepaths to process original event csv data files

# In[109]:


# checking current working directory
#print(os.getcwd())

# Get current folder and subfolder event data
filepath = os.getcwd() + '/event_data'
print(filepath)

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(filepath,'*'))
    #print(file_path_list)
#print(len(file_path_list))


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[110]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
print(len(file_path_list))
try:    
    # for every filepath in the file path list 
    for f in file_path_list:
        print(f)
    # reading csv file 
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            # creating a csv reader object 
            csvreader = csv.reader(csvfile) 
            next(csvreader)

     # extracting each data row one by one and append it        
            for line in csvreader:
                #print(line)
                full_data_rows_list.append(line) 

    # uncomment the code below if you would like to get total number of rows 
    # print(len(full_data_rows_list))
    # uncomment the code below if you would like to check to see what the list of event data rows will look like
    # print(full_data_rows_list)

    # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
    # Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                    'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))
except Exception as e:
    print(e)


# In[111]:


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# 

# #### Creating a Cluster

# In[16]:


# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster(['127.0.0.1'])

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace
# Create a Keyspace 
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)
except Exception as e:
    print(e)
# In[19]:


# Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


# 

# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# In[6]:


# Question 1. (CREATE TABLE)
# Give me [the artist, song title and song's length] 
# in the music app history that was heard during sessionId = 338, and itemInSession = 4

try:
    drop = "DROP TABLE IF EXISTS music_app_hist_by_session_item"
    session.execute(drop)
    query = "CREATE TABLE IF NOT EXISTS music_app_hist_by_session_item "
    query = query + "(sessionId int, itemInSession int, artist text, length float, songTitle text, PRIMARY KEY(sessionId, itemInSession))"
    session.execute(query)
except Exception as e:
    print(e)

                    


# In[7]:


# Question 1. (Insert Data)
# Parse the csv file and insert the data to the table
file = 'event_datafile_new.csv'

try:
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            query = "INSERT INTO music_app_hist_by_session_item (sessionId, itemInSession, artist, length, songTitle)"
            query = query + "VALUES(%s, %s, %s, %s, %s)"
            session.execute(query, (int(line[8]), int(line[3]), line[0], float(line[5]), line[9]))
except Exception as e:
    print(e)


# #### Do a SELECT to verify that the data have been inserted into each table

# In[8]:


# Question 1.
# Query the data from the table and print to verify

query = "SELECT * from music_app_hist_by_session_item WHERE sessionId = 338 and itemInSession = 4"
prettyPrint = PrettyTable()
prettyPrint.field_names = ["Artist Name", "Song Title", "Song Length"]
try:
    rows = session.execute(query)
    for row in rows:
        #print(row)
        prettyPrint.add_row([row.artist, row.songtitle, row.length])
        #print (row.artist, row.songtitle, row.length)
    print(prettyPrint)
except Exception as e:
    print(e)


# 

# In[13]:


# Question 2
# Give me only the following: name of artist, song (sorted by itemInSession)
# and user (first and last name)
# for userid = 10, sessionid = 182

# Step 1: CREATE TABLE
try:
    drop = "DROP TABLE IF EXISTS song_list_by_user_session"
    session.execute(drop)
    query = "CREATE TABLE IF NOT EXISTS song_list_by_user_session "
    query = query + "(userId int, sessionId int, itemInSession int, artist text, firstName text, lastName text, songTitle text, PRIMARY KEY((userid, sessionId), itemInSession))"
    session.execute(query)
except Exception as e:
    print(e)

# Step 2: Insert INTO TABLE
file = 'event_datafile_new.csv'

try:
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            ## Assign the INSERT statements into the `query` variable
            query = "INSERT INTO song_list_by_user_session (userId, sessionId, itemInSession, artist, firstName, lastName, songTitle)"
            query = query + "VALUES(%s, %s, %s, %s, %s, %s, %s)"
            session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[1], line[4], line[9]))
except Exception as e:
    print(e)

# Step 3: Execute Select Statement
try:
    query = "SELECT artist, songTitle, firstName, lastName from song_list_by_user_session WHERE userid = 10 and sessionid = 182"
    prettyPrint = PrettyTable()
    prettyPrint.field_names = ["Artist Name", "Song Title", "First Name", "Last Name"]
    rows = session.execute(query)
    for row in rows:
        #print (row.artist, row.songtitle, row.firstname, row.lastname)
        prettyPrint.add_row([row.artist, row.songtitle, row.firstname, row.lastname])
    print(prettyPrint)
except Exception as e:
    print(e)


# In[22]:


# Question 3: 
# Give me every user name (first and last) in my music app history
# who listened to the song 'All Hands Against His Own'

# Step 1: CREATE TABLE
try:
    drop = "DROP TABLE IF EXISTS user_list_by_song_title"
    session.execute(drop)
    query = "CREATE TABLE IF NOT EXISTS user_list_by_song_title "
    query = query + "(songTitle text, sessionId int, firstName text, lastName text, itemInSession int, PRIMARY KEY(songTitle, sessionId))"
    session.execute(query)
except Exception as e:
    print(e)

# Step 2: Insert INTO TABLE
file = 'event_datafile_new.csv'

try:
    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            ## Assign the INSERT statements into the `query` variable
            query = "INSERT INTO user_list_by_song_title (songTitle, sessionId, firstName, lastName, itemInSession)"
            query = query + "VALUES(%s, %s, %s, %s, %s)"
#             query = "INSERT INTO music_app_hist (artist, firstName, gender, itemInSession, lastName, length, level, location, sessionId, songTitle, userId)
            session.execute(query, (line[9], int(line[8]), line[1], line[4], int(line[3])))
except Exception as e:
    print(e)

# Step 3: Execute Select Statement
try:
    #query = "SELECT * from music_app_hist3 WHERE songtitle = 'All Hands Against His Own' ALLOW FILTERING"
    query = "SELECT firstName, lastName from user_list_by_song_title WHERE songtitle = 'All Hands Against His Own'"
    prettyPrint = PrettyTable()
    prettyPrint.field_names = ["First Name", "Last Name"]
    rows = session.execute(query)
    for row in rows:
        #print (row.firstname, row.lastname)
        prettyPrint.add_row([row.firstname, row.lastname])
    print(prettyPrint)
except Exception as e:
    print(e)


                    


# In[ ]:





# In[ ]:





# ### Drop the tables before closing out the sessions

# In[14]:


# Drop the table before closing out the sessions

try:
    drop = "DROP TABLE IF EXISTS music_app_hist_by_session_item"
    session.execute(drop)
    drop2 = "DROP TABLE IF EXISTS song_list_by_user_session"
    session.execute(drop2)
    drop3 = "DROP TABLE IF EXISTS user_list_by_song_title"
    session.execute(drop3)
except Exception as e:
    print(e)


# In[ ]:





# ### Close the session and cluster connection¶

# In[15]:


session.shutdown()
cluster.shutdown()


# In[ ]:





# In[ ]:




