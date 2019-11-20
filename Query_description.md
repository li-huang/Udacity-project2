# Query 1 requirement: Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

## Query 1 implementation
SELECT artist,song,length 
FROM songplays 
WHERE 
sessionid=338 AND 
iteminsession=4

## How the output data looks like
The output provides artist name, song name and song's length that was heard during sessionid=338 and iteminsession=4
+-----------+---------------------------------+--------------------+
|   Artist  |               Song              |       Length       |
+-----------+---------------------------------+--------------------+
| Faithless | Music Matters (Mark Knight Dub) | 495.30731201171875 |
+-----------+---------------------------------+--------------------+

## Table 1 songplays modeled for Query 1:

CREATE TABLE IF NOT EXISTS songplays 
    (sessionid int, 
    iteminsession int, 
    artist text, 
    song text, 
    length float, 
    PRIMARY KEY(sessionid,iteminsession));

## How I model the table and picked primary key, partition key and clustering key
Table 1 songplays: I started with query requirement. Query needs to pull artist,song,song length data. So these columns should be put in the table. Also query pulls data on criteria of sessionId = 338, and itemInSession = 4. These two columns should also be in the table. Since the column sessionid and iteminsession can uniquely identify a row, these two column can be picked as primary key. 
sessionId will be partition key and itemInsession will be clustering key. Cassandra key order matters. I picked sessionId as partitioned key because in the query requirement this column appears first as a primary searching criteria, then comes iteminInSession.


# Query 2 requirement: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) or userid = 10, sessionid = 182

## Query 2 implementation
SELECT artist,song,firstname, lastname 
FROM user_listened_songs 
WHERE 
userid=10 AND 
sessionid=182

## How the output data looks like
The output will pull the artist names, song names and user first name and last name when userid is 10 and sessioninid is 182.
+-------------------+------------------------------------------------------+------------+-----------+
|       Artist      |                         Song                         | First Name | Last Name |
+-------------------+------------------------------------------------------+------------+-----------+
|  Down To The Bone |                  Keep On Keepin' On                  |   Sylvie   |    Cruz   |
|    Three Drives   |                     Greece 2000                      |   Sylvie   |    Cruz   |
| Sebastien Tellier |                      Kilometer                       |   Sylvie   |    Cruz   |
|   Lonnie Gordon   | Catch You Baby (Steve Pitron & Max Sanna Radio Edit) |   Sylvie   |    Cruz   |
+-------------------+------------------------------------------------------+------------+-----------+

## Table 2 user_listened_songs modeled for Query 1:
CREATE TABLE IF NOT EXISTS user_listened_songs 
    (userid int,
    sessionid int,
    iteminsession int,
    artist text,
    song text,
    firstname text,
    lastname text,
    PRIMARY KEY(userid,sessionid,iteminsession)) 
    WITH CLUSTERING ORDER BY (sessionid ASC,iteminsession ASC);

## How I model the table and picked primary key, partition key and clustering key
Again I started with query requirement. The query asked for song, artist, user name for userid = 10, sessionid = 182 and order songs by itemInsession. So the table should include columns of song, artist, user first name, user last name, userid, session id and iteminsession. Among these columns I considered userid and sessionid should be part of primary key as they are in the matching criteria. But these two columns can't uniquely identify a row as one user and one session can have many songs. So I need to add iteminsession in the primary key. With primary key of these three columns I get only one song. User id is partition key and the other two columns are clustering key as required in query searching criteria. iteminsession will help to order the songs as with given userid and sessionid, data is ordered by iteminsession.


# Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

## Query 3 implementation
SELECT firstname, lastname 
FROM songs_listened 
WHERE 
song = 'All Hands Against His Own'

## How the output data looks like
The output will provide first name and last name for all users who listened to the song of "All Hands Against His Own"

+------------+-----------+
| First Name | Last Name |
+------------+-----------+
| Jacqueline |   Lynch   |
|   Tegan    |   Levine  |
|    Sara    |  Johnson  |
+------------+-----------+

## Table 3 songs_listened modeled for query 3     
CREATE TABLE IF NOT EXISTS songs_listened 
    (song text,
    userid int,
    firstname text,
    lastname text,
    PRIMARY KEY(song,userid));    
    
## How I model the table and picked primary key, partition key and clustering key
From the query requirement at least the columns song, firstname, lastname should be in the table. Since user first name, last name can't identify a person, an extra column userid should also be included. When picking primary key I also looked at the query requirement, the song should be in the primary key. With only song column there are many users, so need to add userid to make a row  unique. So the primary key will be song and userid. In these two column I picked song as partition key and userid as clustering key because the query search only song, it has to be the partition key. Otherwise we have to include userid in the WHERE clause. 
