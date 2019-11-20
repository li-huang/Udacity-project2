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

# In[2]:


# Get the current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Initialize file_path_list
file_path_list=[]

# Loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    # Join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*.csv'))


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[27]:


# Initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
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
            full_data_rows_list.append(line) 
            
# Total number of rows 
print(len(full_data_rows_list))

# creating a smaller event data csv file event_datafile_new.csv that will be used to insert data into the Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[3]:


# check the number of rows in created csv file
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

# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# In[3]:


from cassandra.cluster import Cluster
cluster = Cluster(['127.0.0.1'])

# Create session to establish connection and begin executing queries
session = cluster.connect()


# #### Create Keyspace

# In[4]:


# Create a Keyspace sparkify
try:
    session.execute("CREATE KEYSPACE IF NOT EXISTS sparkify                     WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
                
except Exception as e:
    print(e)


# #### Set Keyspace

# In[5]:


# Set KEYSPACE to the sparkify
try:
    session.set_keyspace('sparkify')
except Exception as e:
    print(e)


# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

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


## Model the table to answer Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4

try:
    query = "CREATE TABLE IF NOT EXISTS songplays     (sessionid int,     iteminsession int,     artist text,     song text,     length float,     PRIMARY KEY(sessionid,iteminsession))"
    session.execute(query)
except Exception as e:
    print(e)
                    


# In[7]:


file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    # skip header
    next(csvreader) 
    for line in csvreader:
        query = "INSERT INTO songplays (sessionid,iteminsession,artist,song,length) "
        query = query + "VALUES (%s,%s,%s,%s,%s)"
        session.execute(query, (int(line[8]), int(line[3]),line[0],line[9],float(line[5])))


# #### Do a SELECT to verify that the data have been inserted into each table

# In[10]:


## Verify the data was inserted into the table
query1 = "SELECT artist,song,length FROM songplays WHERE sessionid=338 AND iteminsession=4"

try:
    rows = session.execute(query1)
except Exception as e:
    print(e)

t = PrettyTable(['Artist','Song','Length'])    
for row in rows:
    t.add_row([row[0], row[1], row[2]])
print(t)


# ### COPY AND REPEAT THE ABOVE THREE CELLS FOR EACH OF THE THREE QUESTIONS

# In[11]:


## Model the table to answer Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
try:
    query = "CREATE TABLE IF NOT EXISTS user_listened_songs     (userid int,    sessionid int,    iteminsession int,    artist text,    song text,    firstname text,    lastname text,    PRIMARY KEY((userid,sessionid),iteminsession));"
    
    session.execute(query)
except Exception as e:
    print(e)

file = 'event_datafile_new.csv'

# Extract data from csv files and insert values into the table
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO user_listened_songs (userid,sessionid,iteminsession,artist,song,firstname,lastname) "
        query = query + "VALUES (%s,%s,%s,%s,%s,%s,%s)"
        session.execute(query, (int(line[10]), int(line[8]),int(line[3]),line[0],line[9],line[1],line[4]))

# Check data has been inserted into table
query2 = "SELECT artist,song,firstname, lastname FROM user_listened_songs WHERE userid=10 AND sessionid=182"
try:
    rows = session.execute(query2)
except Exception as e:
    print(e)
    
t = PrettyTable(['Artist','Song','First Name','Last Name'])
for row in rows:
    t.add_row([row[0], row[1], row[2], row[3]])
print(t)


# In[12]:


## Model table to asnswer Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
try:
    query = "CREATE TABLE IF NOT EXISTS songs_listened     (song text,    userid int,    firstname text,    lastname text,    PRIMARY KEY(song,userid));"
    
    session.execute(query)
except Exception as e:
    print(e)
    
file = 'event_datafile_new.csv'

# Extract data from csv files and insert values into the table
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO songs_listened (song,userid,firstname,lastname) "
        query = query + "VALUES (%s,%s,%s,%s)"
        session.execute(query, (line[9],int(line[10]),line[1],line[4]))

# Check if the table has correct data
query3 = "SELECT firstname, lastname FROM songs_listened WHERE song = 'All Hands Against His Own'"

                        
try:
    rows = session.execute(query3)
except Exception as e:
    print(e)

t = PrettyTable(['First Name','Last Name'])
for row in rows:
    t.add_row([row[0], row[1]])      
print(t)
    


# In[ ]:





# In[ ]:





# ### Drop the tables before closing out the sessions

# In[47]:


try:
    session.execute("DROP TABLE IF EXISTS songplays")
    session.execute("DROP TABLE IF EXISTS user_listened_songs")
    session.execute("DROP TABLE IF EXISTS songs_listened")
except Exception as e:
    print(e)


# In[ ]:





# ### Close the session and cluster connection¶

# In[48]:


session.shutdown()
cluster.shutdown()


# In[ ]:





# In[ ]:




