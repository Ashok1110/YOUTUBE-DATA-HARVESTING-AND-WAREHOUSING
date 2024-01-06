import streamlit as st
from googleapiclient.discovery import build
from pprint import pprint
api_key='AIzaSyDpnNz5D-XKcs6vetcatr7XlwvIaY2DDXc'


youtube =build("youtube", "v3", developerKey=api_key)
def channel_details(channel_id):
    
    request = youtube.channels().list(
         part="snippet,contentDetails,statistics",
         id=channel_id
    )
    response = request.execute()
    channel_id=response['items'][0]['id']
    channel_name=response['items'][0]['snippet']['title']
    channel_description=response['items'][0]['snippet']['description']
    channel_publishedAt=response['items'][0]['snippet']['publishedAt']
    channel_playlist=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    channel_scount=response['items'][0]['statistics']['subscriberCount']
    channel_videocount=response['items'][0]['statistics']['videoCount']
    channel_viewcount=response['items'][0]['statistics']['viewCount']
    d={
        'channel_id':channel_id,
        'channel_name':channel_name,
        'channel_des':channel_description,
        'channel_pubAt':channel_publishedAt,
        'channel_plyist':channel_playlist,
        'channel_snt':channel_scount,
        'channel_vidc':channel_videocount,
        'channel_viewc':channel_viewcount
      }
    return d


 #Video Details

def video_details(video_id):
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
    )
    response = request.execute()

    video_info = response['items'][0]
    statistics = response['items'][0].get('statistics', {})

    channel_id = video_info['snippet']['channelId']
    channel_name = video_info['snippet']['channelTitle']
    video_name = video_info['snippet']['title']
    video_description = video_info['snippet']['description']
    video_published_at = video_info['snippet']['publishedAt']
    video_tags = ','.join(video_info['snippet'].get('tags', []))
    video_view_count = statistics.get('viewCount', 0)
    video_like_count = statistics.get('likeCount', 0)
    video_favorite_count = video_info['statistics']['favoriteCount']
    video_comment_count = statistics.get('commentCount', 0)
    video_duration = video_info['contentDetails']['duration']
    video_thumbnail = video_info['snippet']['thumbnails']
    video_caption_status = video_info['contentDetails']['caption']

    v = {
        'channel_id': channel_id,
        'channel_name': channel_name,
        'video_id': video_id,
        'video_name': video_name,
        'video_description': video_description,
        'video_published_at': video_published_at,
        'video_tags': video_tags,
        'video_view_count': video_view_count,
        'video_like_count': video_like_count,
        'video_favorite_count': video_favorite_count,
        'video_comment_count': video_comment_count,
        'video_duration': video_duration,
        'video_thumbnail': video_thumbnail,
        'video_caption_status': video_caption_status
    }

    return v
# Get Total channel videos
def getchannel_videos(channel_id):
    video_ids=[]
    request = youtube.channels().list(
        part="contentDetails",
        id=channel_id)
    response = request.execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
    
    while True:
        request = youtube.playlistItems().list(playlistId=playlist_id,part="snippet",maxResults=50,pageToken=next_page_token) #maxResults=50
        response = request.execute()
        
        
        for i in range (len(response["items"])):
            video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids

# comment details

def comment_details(videos):
    comment_data=[]
    try:
        for vid in videos:
            request = youtube.commentThreads().list(part="snippet",videoId=vid,maxResults=50)
            response = request.execute()
            for item in response['items']:
                data=dict(comment_id=item['snippet']['topLevelComment']['id'],
                        comment_videoid=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textOriginal'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_PublishedAt=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_data.append(data)
    except:
        pass
    return comment_data

 

#MongoDB 
import pymysql 
import pymongo
from pymongo import MongoClient
import pandas as pd
client = MongoClient("mongodb://localhost:27017")
db = client["project"]
collection = db["channel_Details"]



def main(channel_id):
    
    videos = getchannel_videos(channel_id)
    channels = channel_details(channel_id)
    video_details_result = [video_details(video_id) for video_id in videos]
    comment_details_data = comment_details(videos)

    data = {'channel_info': channels, 'video_details': video_details_result, 'comment_details': comment_details_data}
    
    collection.insert_one(data)

    return "Uploaded successfully"



#  MySQL
import pymysql
from pymongo import MongoClient
import pandas as pd
connection_params = {
'host': 'localhost',
'user': 'root',
'password': 'Reya@2019',
'database': 'project'
}
client = MongoClient("mongodb://localhost:27017")
mongo_db = client["project"]
collection = mongo_db["channel_Details"]
connection_mysql = pymysql.connect(**connection_params)
cursor = connection_mysql.cursor()
# Channel Table creation
def channels_table():
    
    drop_query = '''DROP TABLE IF EXISTS channeldetails'''
    cursor.execute(drop_query)
    connection_mysql.commit()

    
    create_query = '''CREATE TABLE IF NOT EXISTS channeldetails (
                        channel_id VARCHAR(100) PRIMARY KEY,
                        channel_name VARCHAR(100),
                        channel_description TEXT,
                        channel_publishedAt VARCHAR(50),
                        channel_playlist VARCHAR(50),
                        channel_scount BIGINT,
                        channel_videocount INT,
                        channel_viewcount BIGINT
                    )'''

    cursor.execute(create_query)
    connection_mysql.commit()

    
    channel_list = []
    db = client["project"]
    collection = db["channel_Details"]

    for c in collection.find({}, {'_id': 0, 'channel_info': 1}):
        channel_list.append(c['channel_info'])

    df = pd.DataFrame(channel_list)

    for index, row in df.iterrows():
        insert_query = '''
            INSERT INTO channeldetails (
                channel_id,
                channel_name,
                channel_description,
                channel_publishedAt,
                channel_playlist,
                channel_scount,
                channel_videocount,
                channel_viewcount
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        '''

        values = (
            row['channel_id'],
            row['channel_name'],
            row['channel_des'],
            row['channel_pubAt'].replace("T", "").replace("Z", ""),
            row['channel_plyist'],
            row['channel_snt'],
            row['channel_vidc'],
            row['channel_viewc']
        )

        cursor.execute(insert_query, values)
        connection_mysql.commit()

# Videos Table Creation
def videos_table():
    drop_query = '''DROP TABLE IF EXISTS video_details'''
    cursor.execute(drop_query)
    connection_mysql.commit()
    
    create_query = '''CREATE TABLE IF NOT EXISTS videodetails (
                        channel_id VARCHAR(500),
                        channel_name VARCHAR(50),
                        video_id VARCHAR(50) PRIMARY KEY,
                        video_name VARCHAR(500),
                        video_description VARCHAR(1000),
                        video_published_at VARCHAR(100),
                        video_tags VARCHAR(500),
                        video_view_count BIGINT,
                        video_like_count BIGINT,
                        video_favorite_count INT,
                        video_comment_count BIGINT,
                        video_duration VARCHAR(100),
                        video_thumbnail VARCHAR(100),
                        video_caption_status VARCHAR(50)
                    )'''

    cursor.execute(create_query)
    connection_mysql.commit()
    
    video_list = []
    db = client["project"]
    collection = db["channel_Details"]

    for v in collection.find({}, {'_id': 0, 'video_details': 1}):
        for i in range(len(v['video_details'])):
            video_list.append(v['video_details'][i])

    df2 = pd.DataFrame(video_list)

    for index, row in df2.iterrows():
        insert_query = '''INSERT INTO videodetails (
                            channel_id,
                            channel_name,
                            video_id,
                            video_name,
                            video_description,
                            video_published_at,
                            video_tags,
                            video_view_count,
                            video_like_count,
                            video_favorite_count,
                            video_comment_count,
                            video_duration,
                            video_thumbnail,
                            video_caption_status
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON DUPLICATE KEY UPDATE
                        channel_id=VALUES(channel_id),
                        channel_name=VALUES(channel_name),
                        video_id=VALUES(video_id),
                        video_name=VALUES(video_name),
                        video_description=VALUES(video_description),
                        video_published_at=VALUES(video_published_at),
                        video_tags=VALUES(video_tags),
                        video_view_count=VALUES(video_view_count),
                        video_like_count=VALUES(video_like_count),
                        video_favorite_count=VALUES(video_favorite_count),
                        video_comment_count=VALUES(video_comment_count),
                        video_duration=VALUES(video_duration),
                        video_thumbnail=VALUES(video_thumbnail),
                        video_caption_status=VALUES(video_caption_status)'''
                        
                        

        values = (
            row['channel_id'],
            row['channel_name'],
            row['video_id'],
            row['video_name'],
            row['video_description'][:500],
            row['video_published_at'].replace('T', '').replace('Z', ''),
            row['video_tags'][:500],
            row['video_view_count'],
            row['video_like_count'],
            row['video_favorite_count'],
            row['video_comment_count'],
            row['video_duration'].replace('PT', '').replace('H', ':').replace('M', ':').split('S')[0],
            row['video_thumbnail']['default']['url'],
            row['video_caption_status']
        )

        cursor.execute(insert_query, values)
        connection_mysql.commit()

#Comments Table Creation

def comments_table():
    drop_query='''DROP TABLE IF EXISTS commentsdetails'''
    cursor.execute(drop_query)
    connection_mysql.commit()
    create_query = '''CREATE TABLE IF NOT EXISTS commentsdetails (
                        comment_id VARCHAR(100) PRIMARY KEY,
                        comment_videoid  VARCHAR(100),
                        Comment_Text TEXT,
                        Comment_Author VARCHAR(50),
                        Comment_PublishedAt VARCHAR(100)
                    )'''

    cursor.execute(create_query)
    connection_mysql.commit()

    comments_list = []
    db = client['project']
    collection = db["channel_Details"]
    for c in collection.find({}, {'_id': 0, 'comment_details': 1}):
        for i in range(len(c['comment_details'])):
            comments_list.append(c['comment_details'][i])
    df3 = pd.DataFrame(comments_list)

    insert_query = '''INSERT INTO commentsdetails (
                        comment_id,
                        comment_videoid,
                        Comment_Text,
                        Comment_Author,
                        Comment_PublishedAt
            )
            VALUES (%s, %s, %s, %s, %s)
        '''


    for index, row in df3.iterrows():
        values = (
            row['comment_id'],
            row['comment_videoid'],
            row['Comment_Text'],
            row['Comment_Author'],
            row['Comment_PublishedAt'].replace("T", "").replace("Z", ""))

        cursor.execute(insert_query, values)
        connection_mysql.commit()
        
# Total Table main function
def tabels():
    channels_table()
    videos_table()
    comments_table()
    
    return "Tables created succesfully"



def show_channel_table():
    
    channel_list = []
    db = client["project"]
    collection = db["channel_Details"]
    for c in collection.find({}, {'_id': 0, 'channel_info': 1}):
        channel_list.append(c['channel_info'])
    df = st.dataframe(channel_list)
    return df

def show_video_table():
    video_list = []
    db = client["project"]
    collection = db["channel_Details"]
    for v in collection.find({}, {'_id': 0, 'video_details': 1}):
        for i in range(len(v['video_details'])):
            video_list.append(v['video_details'][i])

    df2 = st.dataframe(video_list)
    return df2
def show_comments_table():
    comments_list = []
    db = client['project']
    collection = db["channel_Details"]
    for c in collection.find({}, {'_id': 0, 'comment_details': 1}):
        for i in range(len(c['comment_details'])):
            comments_list.append(c['comment_details'][i])
    df3 = st.dataframe(comments_list)
    return df3

#Stremlit part

st.header(":red[YOUTUBE DATA HAVERSTING  AND WAREHOUSING]")
with st.sidebar:
    st.header("Skills Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("Api Integration")
    st.caption("Streamlit")
    st.caption("Data management using MongoDB and SQL")

channel_id=st.text_input("Enter Channel ID")

if st.button("Display Channel Details"):
    insert=channel_details(channel_id)
    st.success(insert)
    

if st.button("Migrate to Mongodb"):
    ch_ids=[]
    db = client['project']
    collection = db["channel_Details"]
    for ch_data in collection.find({},{'_id':0,"channel_info":1}):
        ch_ids.append(ch_data["channel_info"]["channel_id"])

    if channel_id in ch_ids:
       st.success("channel details of the given channel id already exists")

    else:
        insert=main(channel_id)
        st.success(insert)
        

    
        

if st.button("Migrate to Sql"):
    Table=tabels()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","VIDEOS","COMMENTS"))
if show_table=="CHANNELS":
    show_channel_table()

elif show_table=="VIDEOS":
    show_video_table()

elif show_table=="COMMENTS":
    show_comments_table()

#SQL Connection
connection_params = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Reya@2019',
    'database': 'project'
}
connection_mysql = pymysql.connect(**connection_params)
cursor = connection_mysql.cursor()

# SQL Questions

question=st.selectbox("Select your question",("1.What are the names of all the videos and their corresponding channels?",
                                              "2.Which channels have the most number of videos, and how many videos do they have?",
                                              "3.What are the top 10 most viewed videos and their respective channels?",
                                              "4.How many comments were made on each video, and what are their corresponding video names?",
                                              "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                              "6.What is the total number of likes for each video, and what are their corresponding video names?",
                                              "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                              "8.What are the names of all the channels that have published videos in the year 2022?",
                                              "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                              "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))
# Insert Answers
#Q1
if question== "1.What are the names of all the videos and their corresponding channels?":
    query1='''select video_name,channel_name from videodetails order by channel_name'''
    cursor.execute(query1)
    connection_mysql.commit() 
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["videotitle","channelname"])
    st.write(df)
#Q2
elif question== "2.Which channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_name,channel_videocount from channeldetails order by channel_videocount desc'''
    cursor.execute(query2)
    connection_mysql.commit() 
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)
#Q3
elif question== "3.What are the top 10 most viewed videos and their respective channels?":
    query3='''select video_view_count,channel_name,video_name from videodetails 
          where video_view_count is not null order by video_view_count desc limit 10'''
    cursor.execute(query3)
    connection_mysql.commit() 
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["videoviewcount","channel name","video name"])
    st.write(df3)
#Q4
elif question== "4.How many comments were made on each video, and what are their corresponding video names?":
    query4='''select video_comment_count,video_name from videodetails where video_comment_count is not null'''
    cursor.execute(query4)
    connection_mysql.commit() 
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["videocommentcount","video name"])
    st.write(df4)
#Q5
elif question== "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select video_name,channel_name,video_like_count from videodetails 
            where video_like_count is not null order by video_like_count desc '''
    cursor.execute(query5)
    connection_mysql.commit() 
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videoname","channelname","videolikescount"])
    st.write(df5)

#Q6
elif question== "6.What is the total number of likes for each video, and what are their corresponding video names?":
    query6='''select video_like_count,video_name from videodetails'''
    cursor.execute(query6)
    connection_mysql.commit() 
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["videolikescount","videoname"])
    st.write(df6)

#Q7
elif question== "7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query7='''select channel_name,channel_viewcount from channeldetails'''
    cursor.execute(query7)
    connection_mysql.commit() 
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","channelviewcount"])
    st.write(df7)

#Q8
elif question== "8.What are the names of all the channels that have published videos in the year 2022?":
    query8='''select channel_name from videodetails where video_published_at = 2022''' 
    cursor.execute(query8)
    connection_mysql.commit() 
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["Channel name"])
    st.write(df8)

#Q9
elif question== "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''select channel_name,AVG(video_duration) as avgvideoduration from videodetails group by channel_name''' 
    cursor.execute(query9)
    connection_mysql.commit() 
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channel name","avgvideoduration"])
    #st.write(df9)
    
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channel name"]
        avgvediduration=row["avgvideoduration"]
        Avg_duration_str=str(avgvediduration)
        T9.append(dict(channeltitle=channel_title,avgduration=Avg_duration_str))
    df09=pd.DataFrame(T9)
    st.write(df09)


#Q10
elif question == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10= '''select channel_name,video_name,video_comment_count from videodetails
                where video_comment_count is not null order by video_comment_count desc ''' 
    cursor.execute(query10)
    connection_mysql.commit() 
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["channel name","video name","totalcommentscount"])
    st.write(df10)



   








    

    

