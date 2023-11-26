from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pymongo.server_api import ServerApi
from sqlalchemy import create_engine
from pymongo import MongoClient
from datetime import datetime
import streamlit as stl
import pandas as pd
import sqlalchemy
import pymongo
import urllib
import time
import html
import re

stl.set_page_config(page_title="Youtube ETL Project ", page_icon=":eagle:", layout="wide", initial_sidebar_state="auto",menu_items=None)
stl.title("***:green[YouTube Data Harvesting & Warehousing]🛰️***")

API_KEY='AIzaSyCw3SaPfeSU_NkZraoLKiJvgjNsnqYTSUQ'
youtube = build('youtube', 'v3', developerKey=API_KEY)

# List of Channel Ids:

channel_ids=['UCF6H0li8VwQ9BzmVYRQKvqg',
             'UCz-SkYwTxLdcYF6efMrtssg',
             'UCgLFJc-uJMP-wPZ4LZgnsIQ',
             'UC6NwbJMKTGFu04ZBeNInF6w',
             'UCy3bM_3LiwYSii39gDkvYkg',
             'UCwfvXT2hx7n0NcexGn-FFzQ',
             'UCjx376_46XnogbgARe9g9iw',
             'UCQpgJad_YaHAW_CVFTBNyiw',
             'UCWz-VAT-AF1RUGvk8_y_naQ'
             ]
            
# ELT Project Dashboard

stl.header(':red[***Data Retrival***]')
stl.write ('***(☝️Collect data from :red[YOUTUBE API] by using channel ids and stores it in the :green[MongoDB] database.)***')
channel_id = stl.text_input("***Enter the channel Id***")

# Function to get the channel_details:

def channel_statistics(youtube,channel_ids):
    all_data = []
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=channel_ids)
    response = request.execute()

    for i in range(len(response["items"])):
        data = dict(channel_id = response["items"][i]["id"],
                    channel_name = response["items"][i]["snippet"]["title"],
                    channel_views = response["items"][i]["statistics"]["viewCount"],
                    subscriber_count = response["items"][i]["statistics"]["subscriberCount"],
                    total_videos = response["items"][i]["statistics"]["videoCount"],
                    channel_description = response["items"][i]["snippet"]["description"],
                    playlist_id = response["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"])
        all_data.append(data)
    return all_data

channel_details = channel_statistics(youtube,channel_ids)
df = pd.DataFrame(channel_details)


# Function to get playlist data

def get_playlist_data(df):
    playlist_ids = []

    for i in df["playlist_id"]:
        playlist_ids.append(i)

    return playlist_ids

playlist_id_data = get_playlist_data(df)


# Function to get video ids:

def get_video_ids(youtube,playlist_id_data):
    video_id = []

    for i in playlist_id_data:
        next_page_token = None
        more_pages = True

        while more_pages:
            request = youtube.playlistItems().list(
                        part = 'contentDetails',
                        playlistId = i,
                        maxResults = 50,
                        pageToken = next_page_token)
            response = request.execute()

            for j in response["items"]:
                video_id.append(j["contentDetails"]["videoId"])

            next_page_token = response.get("nextPageToken")
            if next_page_token is None:
                more_pages = False
    return video_id

video_id = get_video_ids(youtube,playlist_id_data)


# Function to get Video details:

def get_video_details(youtube, video_id):
    all_video_stats = []

    for i in range(0, len(video_id), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(video_id[i:i + 50])
        )
        response = request.execute()

        def convert_duration(duration):
            regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
            match = re.match(regex, duration)
            if not match:
                return '00:00:00'
            hours, minutes, seconds = match.groups()
            hours = int(hours[:-1]) if hours else 0
            minutes = int(minutes[:-1]) if minutes else 0
            seconds = int(seconds[:-1]) if seconds else 0
            total_seconds = hours * 3600 + minutes * 60 + seconds
            return '{:02d}:{:02d}:{:02d}'.format(
                int(total_seconds / 3600), int((total_seconds % 3600) / 60), int(total_seconds % 60)
            )

        for video in response["items"]:
            duration = video.get('contentDetails', {}).get('duration', 'Not Available')
            if duration != 'Not Available':
                duration = convert_duration(duration)
                video['contentDetails']['duration'] = duration

            published_dates = video["snippet"]["publishedAt"]
            parsed_dates = datetime.strptime(published_dates, '%Y-%m-%dT%H:%M:%SZ')
            format_date = parsed_dates.strftime('%Y-%m-%d')

            videos = dict(
                video_id=video["id"],
                channel_id=video["snippet"]["channelId"],
                video_name=video["snippet"]["title"],
                published_date=format_date,
                view_count=video["statistics"].get("viewCount", 0),
                Description=video['snippet']['description'],
                like_count=video["statistics"].get("likeCount", 0),
                Thumbnail=video['snippet']['thumbnails']['default']['url'],
                Favorite_Count=video['statistics']['favoriteCount'],
                comment_count=video["statistics"].get("commentCount", 0),
                duration=video["contentDetails"]["duration"],
                Caption_Status=video['contentDetails']['caption']
            )
            all_video_stats.append(videos)

    return all_video_stats

video_details = get_video_details(youtube, video_id)
video_ids = [video['video_id'] for video in video_details]


# Function to get Comment details:

def get_comments(youtube, video_ids):
    comments_data = []
    try:
        next_page_token = None
        for i in video_ids:
            while True:
                try:
                    request = youtube.commentThreads().list(
                        part="snippet,replies",
                        videoId=i,
                        textFormat="plainText",
                        maxResults=100,
                        pageToken=next_page_token
                    )
                    response = request.execute()

                    for item in response["items"]:
                        published_date = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                        parsed_dates = datetime.strptime(published_date, '%Y-%m-%dT%H:%M:%SZ')
                        format_date = parsed_dates.strftime('%Y-%m-%d')

                        comment_text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                        cleaned_comment_text = re.sub(r'[^a-zA-Z0-9\s]', '', comment_text)

                        comments = dict(
                            comment_id=item["id"],
                            video_id=item["snippet"]["videoId"],
                            comment_text=cleaned_comment_text,
                            comment_author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            comment_published_date=format_date
                        )
                        comments_data.append(comments)

                    next_page_token = response.get('nextPageToken')
                    if next_page_token is None:
                        break

                except HttpError as e:
                    if e.resp.status == 403 and "commentsDisabled" in e.content.decode('utf-8'):
                        print(f"Comments are disabled for video with id {i}. Skipping...")
                        break
                    else:
                        raise

    except Exception as e:
        print(f"An error occurred: {e}")

    return comments_data

channel_details = channel_statistics(youtube,channel_ids)
playlist_id_data = get_playlist_data(df)
video_id = get_video_ids(youtube,playlist_id_data)
video_details = get_video_details(youtube,video_id)
comment_details = get_comments(youtube, video_ids)


# Connecting with MongoDB to upload Channel details dervied from Youtube API

username = "preethimahe55"
password = "vmt6par7"
encoded_username = urllib.parse.quote_plus(username)
encoded_password = urllib.parse.quote_plus(password)
uri = f"mongodb+srv://preethimahe55:{encoded_password}@cluster0.ofxbr4b.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(uri)
db = client["YOUTUBE_ETL_PROJECT"]
collection_channel = db['CHANNEL_DATA']
collection_video = db['VIDEO_DATA']
collection_comment = db['COMMENT_DATA']
collection_channel.insert_one({"channel_information": channel_details})
collection_video.insert_one({"video_information": video_details})
collection_comment.insert_one({"comment_information": comment_details})

# Retrieve data from MongoDB

channel_cursor = collection_channel.find({}, {"_id": 0, "channel_information": 1})
channel_data = [entry["channel_information"] for entry in channel_cursor]
flat_channel_data = [item for sublist in channel_data for item in sublist]

video_cursor = collection_video.find({}, {"_id": 0, "video_information": 1})
video_data = [entry["video_information"] for entry in video_cursor]
flat_video_data = [item for sublist in video_data for item in sublist]

comment_cursor = collection_comment.find({}, {"_id": 0, "comment_information": 1})
comment_data = [entry["comment_information"] for entry in comment_cursor]
flat_comment_data = [item for sublist in comment_data for item in sublist]

# Creating DataFrame
channel_Df = pd.DataFrame(flat_channel_data)
video_Df = pd.DataFrame(flat_video_data)
comment_Df = pd.DataFrame(flat_comment_data)


# Gathering data and stored it in the MongoDB

#Streamlit  input query for dashboard
stl.write('''*Gathering data and stored it in the MongoDB to click below* ***:blue['Fetch data and store']***.''')
Fetch_data = stl.button("***Fetch Data & Store Data***")
if Fetch_data:
    with stl.spinner('*Please wait *'):
         time.sleep(5)
         stl.success('***Done!, Data Fetched Successfully***')
    with stl.spinner('*Please wait *'):
         time.sleep(5)
         stl.success('***Done!, Data Uploaded to MONGO DB Successfully***')
         stl.snow()
Fetch_data = stl.button("***Fetch Data from MongoDB  & Converting it to pandas DATAFRAME***")


# Insert DataFrame into SQL

stl.header(':red[Data Migratation]')
stl.write ('''(☝️:- *selected channel data* ***Migrate to :blue[MySQL] database from  :green[MongoDB] database***,
                *if your option unavailable first collect data.*)''')
stl.write('''***Migrate to MySQL database from MongoDB database to click below :blue['Migrate to MySQL'].***''')
Migrate  = stl.button('***Migrate to MySQL***')
if Migrate :
    with stl.spinner('*Please wait *'):
         time.sleep(5)
         stl.success('*Done!, Data Migrated Successfully*')
         stl.snow()

def mysql_connection(channel_Df,video_Df,comment_Df):
    host = "localhost"
    user = "root"
    password = "vmt6par7"
    database = "youtube_etl_project"
    connection = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')


# Inserting channel data into the channel_data table

    channel_Df.to_sql(
       name="channel_data",
       con=connection,
       if_exists="replace",
       index=False
    )
# Inserting video data into the video_data table

    video_Df.to_sql(
       name="video_data",
       con=connection,
       if_exists="replace",
       index=False
    )
# Inserting comment data into the comment_data table

    comment_Df.to_sql(
        name="comment_data",
        con=connection,
        if_exists="replace",  
        index=False)
    
    return "Success"

# Function to Button to fetch and display channel details in Streamlit
# Tabel from db
channel_tabel = 'channel_data'
video_tabel = 'video_data'
comment_tabel = 'comment_data'

# Reading Tabel from SQL 
channel_df = pd.read_sql(channel_tabel, con=engine)
video_df = pd.read_sql(video_tabel, con=engine)
comment_df = pd.read_sql(comment_tabel, con=engine)

# Button to fetch and display channel details
stl.subheader(":orange[***View the user input channel ID's DATA***] ")

def fetch_channel_details(channel_id):
    return channel_df[channel_df['channel_id'] == channel_id]


def fetch_video_details(channel_id):
    return video_df[video_df['channel_id'] == channel_id]


def fetch_comment_details(channel_id):
    video_ids = video_df[video_df['channel_id'] == channel_id]['video_id'].tolist()
    comments = comment_df[comment_df['video_id'].isin(video_ids)]
    return comments

if stl.button("***Fetch Channel Details***"):
    channel_details = fetch_channel_details(channel_id)
    if not channel_details.empty:
        stl.subheader("***Channel Details:***")
        stl.write(channel_details)
    else:
        stl.warning("***Channel not found. Please enter a valid Channel ID.***")



if stl.button("***Fetch Video Details***"):
    video_details = fetch_video_details(channel_id)
    if not video_details.empty:
        stl.subheader("***Video_Details:***")
        stl.write(video_details)
    else:
        stl.warning("***video not found. Please enter a valid Channel ID.***")


if stl.button("***Fetch Comment Details***"):
    comment_details = fetch_comment_details(channel_id)
    if not comment_details.empty:
        stl.subheader("***Comment_Details:***")
        stl.write(comment_details)
    else:
        stl.warning("***comments not found. Please enter a valid Channel ID.***")

stl.subheader(":green[***SELECT THE TABLE FOR VIEW***] ")
show_table = stl.radio("***CLICK THE BUTTONS***",(":green[***Channels***]",":red[***Videos***]",":blue[***Comments***]"))

if show_table == ":green[***Channels***]":
    channel_df
elif show_table ==":red[***Videos***]":
    video_df
elif show_table == ":blue[***Comments***]":
    comment_df

# Data Analysis using SQL Tables

# Execute the query and display the result
if 'query' in locals():
    result = pd.read_sql_query(query, engine)
    stl.write(result)
import pandas as pd
import streamlit as stl
from sqlalchemy import create_engine

# MySQL connection parameters
host = "localhost"
user = "root"
password = "vmt6par7"
database = "youtube_etl_project"

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')

# Streamlit app
stl.title("***YouTube Data Analysis***")

# User selects a question
question = stl.selectbox(
    '***Please Select Your Question***',
    ('1. All the videos and the Channel Name',
     '2. Channels with the most number of videos',
     '3. 10 most viewed videos',
     '4. Comments in each video',
     '5. Videos with the highest likes',
     '6. Likes of all videos',
     '7. Views of each channel',
     '8. Videos published in the year 2022',
     '9. Average duration of all videos in each channel',
     '10. Videos with the highest number of comments'))

# Execute the selected query
if question == '1. All the videos and the Channel Name':
    query = "SELECT v.video_name AS VideoName, c.channel_name AS ChannelName FROM video_data v JOIN channel_data c ON v.channel_id = c.channel_id;"
elif question == '2. Channels with the most number of videos':
    query = "SELECT channel_name AS ChannelName, total_Videos AS NO_Videos FROM channel_data ORDER BY total_Videos DESC;"
elif question == '3. 10 most viewed videos':
    query = "SELECT  c.channel_name AS ChannelName,v.video_name AS VideoName ,v.view_count FROM video_data v JOIN channel_data c ON v.channel_id = c.channel_id WHERE v.view_count > 0 ORDER BY v.view_count DESC LIMIT 10;"
elif question == '4. Comments in each video':
    query = "SELECT video_name AS VideoName, comment_count AS Comments FROM video_data WHERE comment_count >0;"
elif question == '5. Videos with the highest likes':
    query = "SELECT c.channel_name AS ChannelName, v.video_name AS VideoName, v.like_count AS Likes FROM video_data v JOIN channel_data c ON v.channel_id =c.channel_id WHERE v.like_count > 0 ORDER BY v.like_count DESC;"
elif question == '6. Likes of all videos':
    query = "SELECT video_name AS VideoName, like_count AS Likes FROM video_data;"
elif question == '7. Views of each channel':
    query = "SELECT channel_name AS ChannelName, channel_views AS Total_Views FROM channel_data;"
elif question == '8. Videos published in the year 2022':
    query = "SELECT c.channel_name AS ChannelName, v.video_name AS VideoName, v.published_date AS Published_on_2022 FROM video_data v JOIN channel_data c ON v.channel_id =c.channel_id  WHERE EXTRACT(YEAR FROM v.published_date )= 2022;"
elif question == '9. Average duration of all videos in each channel':
    query = "SELECT c. channel_name AS ChannelName, AVG(duration) AS AverageDuration FROM video_data v JOIN channel_data c ON v.channel_id = c.channel_id GROUP BY v.channel_id;"
elif question == '10. Videos with the highest number of comments':
    query = "SELECT video_name AS VideoName, comment_count AS TotalComments FROM video_data WHERE comment_count > 0 ORDER BY comment_count DESC ;"

# Execute the query and display the result as a table
if 'query' in locals():
    result_df = pd.read_sql_query(query, engine)
    stl.table(result_df)
