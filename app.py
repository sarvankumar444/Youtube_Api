from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient import errors
import pymongo
from googleapiclient import errors
from datetime import datetime, timedelta
import psycopg2 as pg
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
from PIL import Image
from pymongo import MongoClient
from datetime import timedelta
import re




api_key="AIzaSyDRQRyHfwqmI-eQ1s-ZinGEMsHVtyz-9Kk"
api_service_name = "youtube"
api_version = "v3"

youtube=build(api_service_name, api_version, developerKey=api_key)



def get_channel_details(youtube, channel_id):
    channel_details = {}
    request = youtube.channels().list(id=channel_id,
                                      part='snippet,statistics')
    response = request.execute()
    for item in response['items']:
        channel_details = {
            'Thumbnail': item['snippet']['thumbnails']['default']['url'],
            'Channel_id': item['id'],
            'Channel_name': item['snippet']['title'],
            'Description_of_channel': item['snippet']['description'],
            'Subscriber_count': item['statistics']['subscriberCount'],
            'Total_views': item['statistics']['viewCount'],
            'Channel_created_date': item['snippet']['publishedAt'][:10],
            'Total_videos_count': item['statistics']['videoCount']

        }
    return channel_details


def get_playlist_id(youtube, channel_id):
    playlist_id_details = []
    next_page_token = None

    # Fetch playlist details
    while True:
        request = youtube.playlists().list(channelId=channel_id, part='snippet,contentDetails',
                                           maxResults=50, pageToken=next_page_token)
        response = request.execute()

        for item in response['items']:
            playlist_detail = {
                'Playlist id': item['id'],
                'Title': item['snippet']['title'],
                'Description': item['snippet']['description'],
                'Video Count': item['contentDetails']['itemCount'],
                'Created Date': item['snippet']['publishedAt'][:10],
                'Created Time': item['snippet']['publishedAt'][11:19],
                'Channel id': item['snippet']['channelId']
            }
            playlist_id_details.append(playlist_detail)

        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break

    return playlist_id_details



def get_video_id(youtube,channel_id):
    video_ids = []
    next_page_token = None

    channel_request = youtube.channels().list(id=channel_id, part='contentDetails')
    channel_response = channel_request.execute()
    upload_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    while True:
        playlist_request = youtube.playlistItems().list(playlistId=upload_playlist_id, part='contentDetails',
                                                        maxResults=50, pageToken=next_page_token)
        playlist_response = playlist_request.execute()

        for item in playlist_response.get('items', []):
            video_id = item['contentDetails']['videoId']
            video_ids.append(video_id)

        next_page_token = playlist_response.get('nextPageToken')
        if not next_page_token:
            break

    return video_ids




def duration_convert(x=None):
    pattern = r'PT(\d+H)?(\d+M)?(\d+S)?'
    find = re.match(pattern, x)
    if find:
        hours, minutes, seconds = find.groups()
        hours = int(hours[:-1]) if hours else 0
        minutes = int(minutes[:-1]) if minutes else 0
        seconds = int(seconds[:-1]) if seconds else 0
        total_seconds = hours * 3600 + minutes * 60 + seconds
        return str(timedelta(seconds=total_seconds))
    else:
        return "00:00:00"

def video_details(youtube, video_ids):
    list_video_details = []
    for video_id in video_ids:
        request = youtube.videos().list(part='snippet,statistics,contentDetails', id=video_id)
        response = request.execute()
        for item in response.get('items', []):
            try:
                details = {
                    'VideoID': item['id'],
                    'title': item['snippet']['title'],
                    'Upload Date': item['snippet']['publishedAt'][:10],
                    'Upload Time': item['snippet']['publishedAt'][11:19],
                    'Description': item['snippet']['description'],
                    'Duration': duration_convert(item['contentDetails']['duration']),
                    'Definition': item['contentDetails']['definition'],
                    'Caption': item['contentDetails']['caption'],
                    'View Count': item['statistics']['viewCount'],
                    'Likes': item['statistics']['likeCount'],
                    'Comments Count': item['statistics']['commentCount'],
                    'Channel id': item['snippet']['channelId']
                }
                list_video_details.append(details)
            except KeyError:
                continue
    return list_video_details

def get_comment(youtube, video_ids):
    list_comment_details = []
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                videoId=video_id,
                part='snippet,replies',
                maxResults=100
            )
            response = request.execute()

            for item in response.get('items', []):
                top_level_comment = item['snippet']['topLevelComment']

                # Check if 'videoId' field is present in the comment data
                video_id = top_level_comment['snippet'].get('videoId', '')
                if not video_id:
                    # Skip comment if 'videoId' is missing
                    continue

                det = {
                    'videoId': video_id,
                    'comment id': item['id'],
                    'comment': top_level_comment['snippet']['textDisplay'],
                    'Comment Date': top_level_comment['snippet']['publishedAt'][:10],
                    'Comment Time': top_level_comment['snippet']['publishedAt'][11:19],
                    'author': top_level_comment['snippet']['authorDisplayName'],
                    'Channel id': top_level_comment['snippet']['channelId']
                }

                list_comment_details.append(det)

                if 'replies' in item:
                    for reply in item['replies']['comments']:
                        det = {
                            'comment id': reply['snippet']['parentId'],
                            'Replies': reply['snippet']['textDisplay'],
                            'Reply Date': reply['snippet']['publishedAt'][:10],
                            'Reply Time': reply['snippet']['publishedAt'][11:19],
                            'Reply Author': reply['snippet']['authorDisplayName'],
                            'ChannelId': reply['snippet']['channelId']
                        }
                        list_comment_details.append(det)
        except HttpError as e:
            if e.resp.status == 403 and b'commentsDisabled' in e.content:
                # Video has disabled comments, skip
                continue
            else:
                # Other HTTP error, raise exception
                raise
        except KeyError as e:
            # Handle missing keys gracefully
            print(f"KeyError: {e}")
            continue

    return list_comment_details

def get_channel_names():
    # Connect to MongoDB
    mongo_client = MongoClient(
        "mongodb+srv://Sarvan26:Sarvan123@project1.dikafru.mongodb.net/?retryWrites=true&w=majority")
    mongo_db = mongo_client["Youtube_project"]

    # Get channel names
    channel_names = [channel['Channel_name'] for channel in mongo_db['Channels'].find({}, {"Channel_name": 1})]
    return channel_names

# Define the function to copy data to MongoDB
def copy_data_to_mongodb(channel_id):
    try:
        # Connect to MongoDB
        mongo_client = MongoClient("mongodb+srv://Sarvan26:Sarvan123@project1.dikafru.mongodb.net/?retryWrites=true&w=majority")
        mongo_db = mongo_client["Youtube_project"]

        # Fetch channel details from YouTube API
        channel_details = get_channel_details(youtube, channel_id)

        # Insert channel details into MongoDB
        if channel_details:
            mongo_db['Channels'].insert_one(channel_details)
            st.success("Channel data copied to MongoDB successfully!")

        # Fetch playlist details from YouTube API
        playlist_details = get_playlist_id(youtube, channel_id)

        # Insert playlist details into MongoDB
        if playlist_details:
            mongo_db['Playlists'].insert_many(playlist_details)
            st.success("Playlist data copied to MongoDB successfully!")

        # Fetch video IDs from YouTube API
        video_ids = get_video_id(youtube, channel_id)

        # Fetch video details from YouTube API
        video_data = video_details(youtube, video_ids)

        # Insert video details into MongoDB
        if video_data:
            mongo_db['Videos'].insert_many(video_data)
            st.success("Video data copied to MongoDB successfully!")

        # Fetch and insert comment data into MongoDB
        for video_id in video_ids:
            video_comment_data = get_comment(youtube, [video_id])

            # Check if there are comments available for the video
            if video_comment_data:
                # Process comment details to match column names
                processed_comment_data = []
                for comment in video_comment_data:
                    processed_comment = {
                        'Comment_id': comment.get('comment id'),
                        'Video_id': comment.get('videoId'),
                        'Comment': comment.get('comment'),
                        'Comment_Date': comment.get('Comment Date'),
                        'Comment_Time': comment.get('Comment Time'),
                        'Author': comment.get('author'),
                        'Channel_id': comment.get('Channel id')
                    }
                    processed_comment_data.append(processed_comment)

                # Insert comment data into MongoDB
                if processed_comment_data:
                    mongo_db['Comments'].insert_many(processed_comment_data)

            else:
                print(f"No comments found for video with ID: {video_id}")

        st.success("Comment data copied to MongoDB successfully!")

    except Exception as e:
        st.error(f"Error copying data to MongoDB: {e}")

# Function to copy data to PostgreSQL
def create_tables():
    try:
        # Connect to PostgreSQL
        conn = pg.connect(
            user="postgres",
            password="abc",
            host="localhost",
            port="5432",
            database="Youtube_project"
        )
        cursor = conn.cursor()

        # Create table for Channel if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Channel (
            Channel_id TEXT PRIMARY KEY,
            Channel_name TEXT,
            Description_of_channel TEXT,
            Subscriber_count INTEGER,
            Total_views INTEGER,
            Channel_created_date DATE,
            Total_videos_count INTEGER
        )
        """)

        # Create table for Playlist if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Playlist (
            Playlist_id TEXT PRIMARY KEY,
            Title TEXT,
            Description TEXT,
            Video_Count INTEGER,
            Created_Date DATE,
            Created_Time TIME,
            Channel_id TEXT,
            FOREIGN KEY (Channel_id) REFERENCES Channel(Channel_id)
        )
        """)

        # Create table for Video if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Video (
                VideoID TEXT PRIMARY KEY,
                title TEXT,
                Upload_Date DATE,
                Upload_Time TIME,
                Description TEXT,
                Duration INTERVAL,
                Definition TEXT,
                Caption BOOLEAN,
                View_Count TEXT,  
                Likes TEXT,       
                Comments_Count TEXT, 
                Channel_id TEXT,
                FOREIGN KEY (Channel_id) REFERENCES Channel(Channel_id)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Comment (
                Comment_id TEXT PRIMARY KEY,
                Video_id TEXT,
                Comment TEXT,
                Comment_Date DATE,
                Comment_Time TIME,
                Author TEXT,
                FOREIGN KEY (Video_id) REFERENCES Video(VideoID)
            )
        """)

        st.success("Tables created successfully!")

    except Exception as e:
        st.error(f"Error creating tables: {e}")


def get_channel_names():
    # Connect to MongoDB
    mongo_client = MongoClient(
        "mongodb+srv://Sarvan26:Sarvan123@project1.dikafru.mongodb.net/?retryWrites=true&w=majority")
    mongo_db = mongo_client["Youtube_project"]

    # Get channel names
    channel_names = [channel['Channel_name'] for channel in mongo_db['Channels'].find({}, {"Channel_name": 1})]
    return channel_names


def copy_data_to_sql(selected_channel):
    try:
        # Call create_tables function to create tables in PostgreSQL
        create_tables()

        # Connect to MongoDB
        mongo_client = MongoClient(
            "mongodb+srv://Sarvan26:Sarvan123@project1.dikafru.mongodb.net/?retryWrites=true&w=majority")
        mongo_db = mongo_client["Youtube_project"]

        # Fetch data from MongoDB for the selected channel
        channel_data = mongo_db['Channels'].find_one({"Channel_name": selected_channel})
        playlist_data = list(mongo_db['Playlists'].find({"Channel id": channel_data['Channel_id']}))
        video_data = list(mongo_db['Videos'].find({"Channel id": channel_data['Channel_id']}))
        comment_data = list(mongo_db['Comments'].find({"Channel_id": channel_data['Channel_id']}))

        # Connect to PostgreSQL
        psql_conn = pg.connect(
            user="postgres",
            password="abc",
            host="localhost",
            port="5432",
            database="Youtube_project"
        )
        psql_cursor = psql_conn.cursor()

        # Insert channel data into PostgreSQL
        psql_cursor.execute(
            "INSERT INTO Channel (Channel_id, Channel_name, Description_of_channel, Subscriber_count, Total_views, Channel_created_date, Total_videos_count) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (Channel_id) DO UPDATE SET "
            "Channel_name = EXCLUDED.Channel_name, "
            "Description_of_channel = EXCLUDED.Description_of_channel, "
            "Subscriber_count = EXCLUDED.Subscriber_count, "
            "Total_views = EXCLUDED.Total_views, "
            "Channel_created_date = EXCLUDED.Channel_created_date, "
            "Total_videos_count = EXCLUDED.Total_videos_count",
            (channel_data['Channel_id'], channel_data['Channel_name'], channel_data['Description_of_channel'],
             channel_data['Subscriber_count'], channel_data['Total_views'], channel_data['Channel_created_date'],
             channel_data['Total_videos_count'])
        )

        # Insert or update playlist data
        for playlist in playlist_data:
            psql_cursor.execute(
                "INSERT INTO Playlist (Playlist_id, Title, Description, Video_Count, Created_Date, Created_Time, Channel_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (Playlist_id) DO NOTHING",
                (playlist['Playlist id'], playlist['Title'], playlist['Description'], playlist['Video Count'],
                 playlist['Created Date'], playlist['Created Time'], playlist['Channel id'])
            )

        # Insert or update video data
        for video in video_data:
            # Convert duration from MongoDB format to SQL format
            duration_sql = duration_convert(video["Duration"])

            # Convert the duration to a string in HH:MI:SS format
            duration_string = str(duration_sql)

            # Insert the video data into the PostgreSQL database
            psql_cursor.execute(
                "INSERT INTO Video (VideoID, title, Upload_Date, Upload_Time, Description, duration, Definition, Caption, View_Count, Likes, Comments_Count, Channel_id) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (VideoID) DO NOTHING",
                (video['VideoID'], video['title'], video['Upload Date'], video['Upload Time'], video['Description'],
                 video['Duration'], video['Definition'], video['Caption'], video['View Count'], video['Likes'],
                 video['Comments Count'], video['Channel id'])
            )

        # Commit changes before proceeding to comments
        psql_conn.commit()

        # Fetch existing video IDs from PostgreSQL
        psql_cursor.execute("SELECT VideoID FROM Video")
        existing_video_ids = [row[0] for row in psql_cursor.fetchall()]

        # Insert comment data into PostgreSQL
        for comment in comment_data:
            if comment['Video_id'] in existing_video_ids:
                psql_cursor.execute(
                    "INSERT INTO Comment (Comment_id, Video_id, Comment, Comment_Date, Comment_Time, Author) "
                    "VALUES (%s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (Comment_id) DO NOTHING",
                    (comment['Comment_id'], comment['Video_id'], comment['Comment'], comment['Comment_Date'],
                     comment['Comment_Time'], comment['Author'])
                )
            else:
                # Handle missing video IDs gracefully
                print(f"Video ID {comment['Video_id']} not found in PostgreSQL. Skipping comment insertion.")

        # Commit changes
        psql_conn.commit()
        st.success("Data copied from MongoDB to PostgreSQL successfully!")

    except pg.Error as e:
        st.error(f"Error copying data from MongoDB to PostgreSQL: {e}")

    finally:
        # Close connections
        if 'psql_conn' in locals():
            psql_cursor.close()
            psql_conn.close()
        if 'mongo_client' in locals():
            mongo_client.close()

def list_channels():
    st.title("List of Channels")
    # Connect to MongoDB
    mongo_client = MongoClient(
        "mongodb+srv://Sarvan26:Sarvan123@project1.dikafru.mongodb.net/?retryWrites=true&w=majority")
    mongo_db = mongo_client["Youtube_project"]

    # Fetch and display the list of channel names
    channel_names = [channel['Channel_name'] for channel in mongo_db['Channels'].find({}, {"Channel_name": 1})]
    st.write(channel_names)


def execute_query(query, title):
    conn = pg.connect(
        user="postgres",
        password="abc",
        host="localhost",
        port="5432",
        database="Youtube_project"
    )
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()

    # Display the query result as a table
    st.subheader(title)
    st.table(rows)


# Define SQL queries
import streamlit as st
import pandas as pd
from pymongo import MongoClient

# Define SQL queries
def query_videos_and_channels():
    return """
    SELECT v.title AS Video_Title, c.Channel_name AS Channel_Name
    FROM Video v
    INNER JOIN Channel c ON v.Channel_id = c.Channel_id
    """

def query_channels_with_most_videos():
    return """
    SELECT c.Channel_name, COUNT(*) AS Video_Count
    FROM Video v
    INNER JOIN Channel c ON v.Channel_id = c.Channel_id
    GROUP BY c.Channel_name
    ORDER BY Video_Count DESC
    LIMIT 1
    """

def query_top_10_viewed_videos():
    return """
    SELECT v.title AS Video_Title, c.Channel_name AS Channel_Name, v.View_Count
    FROM Video v
    INNER JOIN Channel c ON v.Channel_id = c.Channel_id
    ORDER BY v.View_Count DESC
    LIMIT 10
    """

def query_comments_per_video():
    return """
    SELECT v.title AS Video_Title, COUNT(*) AS Comment_Count
    FROM Video v
    INNER JOIN Comment cm ON v.VideoID = cm.Video_id
    GROUP BY v.title
    """

def query_videos_with_highest_likes():
    return """
    SELECT v.title AS Video_Title, c.Channel_name AS Channel_Name, v.Likes
    FROM Video v
    INNER JOIN Channel c ON v.Channel_id = c.Channel_id
    ORDER BY v.Likes DESC
    LIMIT 10
    """

def query_likes_dislikes_per_video():
    return """
    SELECT v.title AS Video_Title, SUM(CAST(v.Likes AS INTEGER)) AS Total_Likes
    FROM Video v
    GROUP BY v.title
    """

def query_total_views_per_channel():
    return """
    SELECT c.Channel_name, SUM(CAST(v.View_Count AS INTEGER)) AS Total_Views
    FROM Video v
    INNER JOIN Channel c ON v.Channel_id = c.Channel_id
    GROUP BY c.Channel_name
    """


def query_channels_published_in_2022():
    return """
    SELECT DISTINCT c.Channel_name
    FROM Channel c
    INNER JOIN Video v ON c.Channel_id = v.Channel_id
    WHERE EXTRACT(YEAR FROM v.Upload_Date) = 2022
    """

def query_average_duration_per_channel():
    return """
    SELECT c.Channel_name, 
       AVG(EXTRACT(EPOCH FROM COALESCE(v.duration, '00:00:00')::interval)) AS Average_Duration_seconds
    FROM Video v
    JOIN Channel c ON v.Channel_id = c.Channel_id
    GROUP BY c.Channel_name;
    """

def query_videos_with_highest_comments():
    return """
    SELECT v.title AS Video_Title, c.Channel_name AS Channel_Name, COUNT(*) AS Comment_Count
    FROM Video v
    INNER JOIN Comment cm ON v.VideoID = cm.Video_id
    INNER JOIN Channel c ON v.Channel_id = c.Channel_id
    GROUP BY v.title, c.Channel_name
    ORDER BY Comment_Count DESC
    LIMIT 10
    """

# Define function to execute SQL queries
def execute_query(sql_query):
    try:
        conn = pg.connect(
            user="postgres",
            password="abc",
            host="localhost",
            port="5432",
            database="Youtube_project"
        )
        cursor = conn.cursor()
        cursor.execute(sql_query)
        result = cursor.fetchall()
        conn.close()

        # Get column names from the cursor description
        columns = [desc[0] for desc in cursor.description]

        # Create DataFrame with fetched data and column names
        df = pd.DataFrame(result, columns=columns)
        return df
    except pg.Error as e:
        st.error(f"Error executing SQL query: {e}")


# Define function to handle SQL query answers
def sql_query_answers():
    st.subheader("SQL Query Answers")
    question_options = [
        "What are the names of all the videos and their corresponding channels?",
        "Which channels have the most number of videos, and how many videos do they have?",
        "What are the top 10 most viewed videos and their respective channels?",
        "How many comments were made on each video, and what are their corresponding video names?",
        "Which videos have the highest number of likes, and what are their corresponding channel names?",
        "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
        "What is the total number of views for each channel, and what are their corresponding channel names?",
        "What are the names of all the channels that have published videos in the year 2022?",
        "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "Which videos have the highest number of comments, and what are their corresponding video names?"
    ]
    selected_question = st.selectbox("Select Question:", question_options)

    if selected_question:
        # Execute the corresponding SQL query based on the selected question
        if selected_question == question_options[0]:
            df = execute_query(query_videos_and_channels())
            st.dataframe(df)  # Display the DataFrame as a Streamlit table
        elif selected_question == question_options[1]:
            df = execute_query(query_channels_with_most_videos())
            st.dataframe(df)
        elif selected_question == question_options[2]:
            df = execute_query(query_top_10_viewed_videos())
            st.dataframe(df)
        elif selected_question == question_options[3]:
            df = execute_query(query_comments_per_video())
            st.dataframe(df)
        elif selected_question == question_options[4]:
            df = execute_query(query_videos_with_highest_likes())
            st.dataframe(df)
        elif selected_question == question_options[5]:
            df = execute_query(query_likes_dislikes_per_video())
            st.dataframe(df)
        elif selected_question == question_options[6]:
            df = execute_query(query_total_views_per_channel())
            st.dataframe(df)
        elif selected_question == question_options[7]:
            df = execute_query(query_channels_published_in_2022())
            st.dataframe(df)
        elif selected_question == question_options[8]:
            df = execute_query(query_average_duration_per_channel())
            st.dataframe(df)
        elif selected_question == question_options[9]:
            df = execute_query(query_videos_with_highest_comments())
            st.dataframe(df)


# Main Streamlit app
def main():
    st.title("Data Migration App")

    # Define the options for the tabs
    options = ["Copy Data to MongoDB", "Copy Data to PostgreSQL", "List Channels", "SQL Query Answers"]

    # Create tabs in the sidebar
    selected_tab = st.sidebar.radio("Select Action:", options)

    # Display the selected tab content
    if selected_tab == "Copy Data to MongoDB":
        st.subheader("Copy Data to MongoDB")
        channel_id = st.text_input("Enter YouTube Channel ID:")
        if st.button("Copy Data to MongoDB"):
            copy_data_to_mongodb(channel_id)

    elif selected_tab == "Copy Data to PostgreSQL":
        st.subheader("Copy Data to PostgreSQL")
        # Get available channel names from MongoDB
        channel_names = get_channel_names()
        # Dropdown to select channel
        selected_channel = st.selectbox("Select Channel", channel_names)
        # Button to trigger data transfer
        if st.button("Copy Data to SQL"):
            copy_data_to_sql(selected_channel)

    elif selected_tab == "List Channels":
        st.subheader("List of Channels")
        # Connect to MongoDB
        mongo_client = MongoClient(
            "mongodb+srv://Sarvan26:Sarvan123@project1.dikafru.mongodb.net/?retryWrites=true&w=majority")
        mongo_db = mongo_client["Youtube_project"]

        # Fetch and display the list of channel names
        channel_names = [channel['Channel_name'] for channel in mongo_db['Channels'].find({}, {"Channel_name": 1})]
        st.write(channel_names)

    elif selected_tab == "SQL Query Answers":
        sql_query_answers()

if __name__ == "__main__":
    main()
