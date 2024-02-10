from googleapiclient.discovery import build
from datetime import datetime
import pymongo
import mysql.connector as mysql
import pandas as pd
import streamlit as st
import re

#Api key connection
def Api_connect():
    Api_Id = "AIzaSyDtqg9EATnMD1jz6MdBVzPG_HgUm3Zqlo4"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=Api_Id)
    return youtube

youtube = Api_connect()

#get channel information
def get_channel_info(channel_id):
    request=youtube.channels().list(
        part ="snippet,contentDetails,Statistics",
        id = channel_id)

    response1 = request.execute()

    for i in range(0,len(response1["items"])):
        data=dict(Channel_Name=response1["items"][i]["snippet"]["title"],
                  Channel_Id=response1["items"][i]["id"],
                  Subscription_Count = response1["items"][i]["statistics"]["subscriberCount"],
                  Views = response1["items"][i]["statistics"]["viewCount"],
                  Total_Videos = response1["items"][i]["statistics"]["videoCount"],
                  Channel_Description = response1["items"][i]["snippet"]["description"],
                  Playlist_Id = response1["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"]
                  )
        return data
    
# channel_info = get_channel_info("UCChmJrVa8kDg05JfCmxpLRw")
# print(channel_info)

def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list( 
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids

# channel_viedo_info = get_channel_videos("UCChmJrVa8kDg05JfCmxpLRw")
# print("channel_video info",channel_viedo_info)

#get video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id= video_id
        )
        response = request.execute()

        for item in response["items"]:
            data =dict(Channel_Name=item['snippet']['channelTitle'],
                        Channel_Id= item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title=item['snippet']['title'],
                        Tags=item ['snippet'].get('tags'),
                        Thumbnail= item['snippet']['thumbnails']['default']['url'],
                        Description=item['snippet'].get('description'),
                        Published_Date = item['snippet']['publishedAt'],
                        Duration=item['contentDetails']['duration'],
                        Views=item['statistics'].get('viewCount'),
                        Likes=item['statistics'].get('likeCount'),
                        Comments=item['statistics'].get('commentCount'),
                        Favorite_Count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption_Status= item['contentDetails']['caption']

                    )
            video_data.append(data)
    return video_data

# channel_video_ids = get_channel_videos("UCChmJrVa8kDg05JfCmxpLRw")
# video_data = get_video_info(channel_video_ids)
# print("Video Data",video_data)

def get_comment_info(video_ids):
        Comment_Information = []
        try:
                for video_id in video_ids:

                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50
                                )
                        response5 = request.execute()
                        
                        for item in response5["items"]:
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                                Comment_Information.append(comment_information)
        except:
                pass
                
        return Comment_Information

# channel_video_ids = get_channel_videos("UCChmJrVa8kDg05JfCmxpLRw")
# Comment_Information = get_comment_info(channel_video_ids)
# print("Comment Information",Comment_Information)

def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data

# channel_playlist_info = get_playlist_info("UCChmJrVa8kDg05JfCmxpLRw")
# print("channel playlist info",channel_playlist_info)

client=pymongo.MongoClient("mongodb://localhost:27017/")
db=client["Youtube_data"]

def channel_details(channel_id):
    ch_details= get_channel_info(channel_id)
    vi_ids= get_channel_videos(channel_id)
    vi_details= get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)
    pl_details= get_playlist_info(channel_id)
    
    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                     "video_information":vi_details,"comment_information":com_details})
   
    return "upload completed successfully"

# print(channel_details("UCChmJrVa8kDg05JfCmxpLRw"))

#Creating Channels Table and insert channel data
def channels_table():
    mydb = mysql.connect(host='127.0.0.1',user='root',passwd='Subhash@5',
                            database="youtube_data",
                            port=3306)
    cursor=mydb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query="""create table if not exists channels (Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscription_Count bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(50))"""
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Channels Table already created")

    ch_list=[]
    db= client["Youtube_data"] 
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
            insert_query='''insert into channels(Channel_Name,
                                                Channel_Id,
                                                Subscription_Count,
                                                Views,
                                                Total_Videos,
                                                Channel_Description,
                                                Playlist_Id)

                                                values(%s,%s,%s,%s,%s,%s,%s)'''
            values=(
                 row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
                st.write("Channels values are already inserted")

# channels_table()

#Create Playlist Table and insert playlist data          
def playlist_table():
    mydb = mysql.connect(host='127.0.0.1',user='root',passwd='Subhash@5',
                            database="youtube_data",
                            port=3306)
    cursor=mydb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query="""create table if not exists playlists (PlaylistId varchar(100) primary key,
                                                                Title varchar(100),
                                                                ChannelId varchar(100),
                                                                ChannelName varchar(100),
                                                                PublishedAt timestamp,
                                                                VideoCount int)"""

        cursor.execute(create_query)
        mydb.commit()
    except:
         st.write("Playlists Table alredy created")

    pl_list=[]
    db= client["Youtube_data"] 
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df=pd.DataFrame(pl_list)


    for index, row in df.iterrows():
            # Convert timestamp string to datetime object
            published_at = datetime.strptime(row['PublishedAt'], "%Y-%m-%dT%H:%M:%SZ")
            insert_query = '''INSERT into playlists (PlaylistId,
                                                            Title,
                                                            ChannelId,
                                                            ChannelName,
                                                            PublishedAt,
                                                            VideoCount
                                                            )
                                                            VALUES (%s, %s, %s, %s, %s, %s)'''

            values = (row['PlaylistId'],
                    row['Title'],
                    row['ChannelId'],
                    row['ChannelName'],
                    published_at,
                    row['VideoCount'])

            try: 
                cursor.execute(insert_query,values)
                mydb.commit()    
            except Exception as e:
                print("An error occurred:", e)
                st.write("Playlists values are already inserted")

# playlist_table()

#Create Videos table and insert data into it
def videos_table():
    mydb = mysql.connect(host='127.0.0.1',user='root',passwd='Subhash@5',
                            database="youtube_data",
                            port=3306)
    cursor=mydb.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query = """create table if not exists videos (
                    Channel_Name VARCHAR(150),
                    Channel_Id VARCHAR(100),
                    Video_Id VARCHAR(50) primary key,
                    Title VARCHAR(150),
                    Tags text,
                    Thumbnail VARCHAR(225),
                    Description TEXT,
                    Published_Date DATETIME,
                    Duration TIME,
                    Views BIGINT,
                    Likes BIGINT,
                    Comments INT,
                    Favorite_Count INT,
                    Definition VARCHAR(10),
                    Caption_Status VARCHAR(50)
                    )"""

    cursor.execute(create_query)
    mydb.commit()
    # except:
    #     st.write("Videos Table alrady created")
    print('Video Table created')
    vi_list=[]
    db= client["Youtube_data"] 
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)

    for index, row in df2.iterrows():
            # Extract hours, minutes, and seconds from the duration string
            duration_str = row['Duration']
            duration_match = re.match(r'PT((\d+)H)?(\d+M)?(\d+S)?', duration_str)
            hours = minutes = seconds = 0
            if duration_match:
                if duration_match.group(2):
                    hours = int(duration_match.group(2))
                if duration_match.group(3):
                    minutes = int(duration_match.group(3)[:-1])
                if duration_match.group(4):
                    seconds = int(duration_match.group(4)[:-1])

            # Format the duration as 'HH:MM:SS' if hours are present, otherwise 'MM:SS'
            if hours > 0:
                duration_formatted = '{:02d}:{:02d}:{:02d}'.format(hours, minutes, seconds)
            else:
                duration_formatted = '{:02d}:{:02d}'.format(minutes, seconds)
            # Convert timestamp string to datetime object
            published_date = datetime.strptime(row['Published_Date'], "%Y-%m-%dT%H:%M:%SZ")
            insert_query = '''INSERT INTO videos (Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    Title,
                                                    Tags,
                                                    Thumbnail,
                                                    Description,
                                                    Published_Date,
                                                    Duration,
                                                    Views,
                                                    Likes,
                                                    Comments,
                                                    Favorite_Count,
                                                    Definition,
                                                    Caption_Status
                                                    )
                                                    values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
            values = (row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    ', '.join(row['Tags']),
                    row['Thumbnail'],
                    row['Description'],
                    published_date,
                    duration_formatted,
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status'])
            
            
            try:
                cursor.execute(insert_query,values)
                mydb.commit() 
            except Exception as e:
                print("An error occurred : ",e)
                st.write("videos values already inserted in the table")

# videos_table()

#Create Comments table and insert data into it
def comments_table():
    mydb = mysql.connect(host='127.0.0.1',user='root',passwd='Subhash@5',
                                database="youtube_data",
                                port=3306)
    cursor=mydb.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = """create table if not exists comments (
                    Comment_Id varchar(100) primary key,
                    Video_Id varchar(80),
                    Comment_Text text,
                    Comment_Author varchar(150),
                    Comment_Published_time timestamp
                )"""
        cursor.execute(create_query)
        mydb.commit()

    except:
        st.write("Commentsp Table already created")

    com_list=[]
    db= client["Youtube_data"] 
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)

    for index, row in df3.iterrows():
            # Convert timestamp string to datetime object
            comment_published_time = datetime.strptime(row['Comment_Published'], "%Y-%m-%dT%H:%M:%SZ")

            # Format datetime object as string in MySQL-compatible format
            comment_published_time_formatted = comment_published_time.strftime("%Y-%m-%d %H:%M:%S")

            insert_query = '''INSERT INTO comments (
                    Comment_Id,
                    Video_Id,
                    Comment_Text,
                    Comment_Author,
                    Comment_Published_time
                )
                VALUES (%s, %s, %s, %s, %s)'''

            values = (row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    comment_published_time_formatted
                    )
            try:
                cursor.execute(insert_query, values)
                mydb.commit()
            except Exception as e:
                print("Error occured ",e)
                st.write("This comments are already exist in comments table")

# comments_table()

#Calling all the table insert funcion calls in one single function                
def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "Tables created successfully"

# tables()

#Function call to display channels table
def show_channels_table():
    ch_list=[]
    db= client["Youtube_data"] 
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    channels_table =st.dataframe(ch_list)
    
    return channels_table

# show_channels_table() 

#Function call to display playlists table
def show_playlists_table():
    pl_list=[]
    db= client["Youtube_data"] 
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    playlists_table=st.dataframe(pl_list)
    
    return  playlists_table

#Function call to display videos table
def show_videos_table():
    vi_list=[]
    db= client["Youtube_data"] 
    coll2=db["channel_details"]
    for vi_data in coll2.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    videos_table=st.dataframe(vi_list)
    
    return videos_table

#Function call to display comments table
def show_comments_table():
    com_list=[]
    db= client["Youtube_data"] 
    coll3=db["channel_details"]
    for com_data in coll3.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    comments_table=st.dataframe(com_list)
    
    return comments_table 


# Streamlit code
st.markdown("<h2 style='color:green;'>YOUTUBE DATA HARVESTING AND WAREHOUSING</h2>", unsafe_allow_html=True)
st.write(":red[Skill Take Away]")
st.write("Python Scripting - Data Collection - MongoDB - API Integration and Data Management using MongoDB and SQL")
    
channel_id= st.text_input("Enter the Channel ID")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Extract and store data"):
    for channel in channels:
        ch_ids=[]
        db=client["Youtube_data"]
        coll1=db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel_id in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
            
        else:
            output=channel_details(channel_id)
            st.success(output)
        
if st.button("Transfer to MySQL"):
    Table= tables()
    st.success(Table)
    
show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"),horizontal=True)

if show_table =="CHANNELS":
    show_channels_table()
    
elif show_table == "PLAYLISTS":
    show_playlists_table()
    
elif show_table == "VIDEOS":
    show_videos_table()
    
elif show_table == "COMMENTS":
    show_comments_table()

##SQL connection
mydb = mysql.connect(host='127.0.0.1',user='root',passwd='Subhash@5',
                          database="youtube_data",
                          port=3306)
cursor=mydb.cursor()

question= st.radio("Question",("1. What are the names of all the videos and their corresponding channel",
                                              "2. Which channels have most number of videos",
                                              "3. Top 10 most viewed videos ",
                                              "4. What are the comments in each videos",
                                              "5. Videos with highest number of likes",
                                              "6. Total no of likes of all videos",
                                              "7. Total no of views of each channel",
                                              "8. List the name of videos published in 2022",
                                              "9. What is the average duration of all videos in each channel",
                                              "10. Which video has highest number of comments"))


if question == "1. What are the names of all the videos and their corresponding channel":
    query1=''' select title as videos, Channel_Name as ChannelName from videos;'''
    cursor.execute(query1)
    #mydb.commit()
    t1 = cursor.fetchall()
    df=pd.DataFrame(t1,columns=["Video Title","Channel Name"])
    st.write(df)
    
elif question == "2. Which channels have most number of videos":
    query2=''' select Channel_Name as channelName,Total_Videos as NO_Videos from channels 
               order by Total_Videos desc;'''
    cursor.execute(query2)
    #mydb.commit()
    t2 = cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["Channel Name","No of videos"])
    st.write(df2)
    
elif question == "3. Top 10 most viewed videos ":
    query3=''' select Views as views,Channel_Name as ChannelName, Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    # mydb.commit()
    t3 = cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel Name","video title"])
    st.write(df3)
    
elif question == "4. What are the comments in each videos":
    query4='''select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;'''
    cursor.execute(query4)
    # mydb.commit()
    t4 = cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["No Of Comments", "Video Title"])
    st.write(df4)
    
elif question == "5. Videos with highest number of likes":
    query5=''' select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    # mydb.commit()
    t5 = cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["video Title","channel Name","like count"])
    st.write(df5)
    
elif question == "6. Total no of likes of all videos":
    query6='''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    # mydb.commit()
    t6 = cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["like count","video title"])
    st.write(df6)
    
elif question == "7. Total no of views of each channel":
    query7='''select Channel_Name as ChannelName, Views as Channelviews from channels;'''
    cursor.execute(query7)
    # mydb.commit()
    t7 = cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","total views"])
    st.write(df7)
    
elif question == "8. List the name of videos published in 2022":
    query8='''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    # mydb.commit()
    t8= cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"])
    st.write(df8)
    
elif question == "9. What is the average duration of all videos in each channel":
    query9='''SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;'''
    cursor.execute(query9)
    # mydb.commit()
    t9= cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])
    
    print("***Subhash***",df9)
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})

    df9=pd.DataFrame(T9)
    st.write(df9)
    
elif question == "10. Which video has highest number of comments":
    query10=''' select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    # mydb.commit()
    t10= cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["Video Title","Channel Name","NO Of Comments"])
    st.write(df10)