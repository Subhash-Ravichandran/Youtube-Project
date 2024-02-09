# YouTube Data Harvesting and Warehousing
YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit
These import statements suggest that the code may involve working with Google APIs, MongoDB, MySQL, regular expressions, Streamlit for creating a web application or dashboard, and data manipulation with JSON and pandas. The code itself likely contains further functionality related to these libraries and modules.

A function is created to connect the API platform to fetch YouTube data as we require.
Channel ID is given as input in the function and the details required are fetched correspondingly.

YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit-Project
To create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.
Skills Takeaway From This Project:
- **Python Scripting:** Python is used for scripting and automation.
- **Data Collection:** Explaining your methods and tools used for data collection.
- **MongoDB:** Database used for storing and managing collected data.
- **Streamlit:** Utilized for data visualization and building interactive dashboards.
- **API Integration:** How APIs are used for data retrieval and integration.
- **Data Management:** Utilizing MongoDB Atlas for NoSQL database and SQL databases for data storage and management.


# About YouTube Data Analysis Streamlit Application

This project is a Streamlit application that facilitates accessing and analyzing data from multiple YouTube channels. The application includes several features to interact with YouTube data via the YouTube API, store it in a MongoDB data lake, migrate it to a SQL database, and perform data retrieval and analysis.

## Features

### 1. Data Retrieval from YouTube Channels
- **Input YouTube Channel ID:** Users can input a YouTube channel ID to retrieve relevant data, including channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, and comments of each video using the YouTube API.

### 2. Storing Data in MongoDB Data Lake
- **Store Retrieved Data:** Users have the option to store the retrieved data in a MongoDB database as a data lake. MongoDB is utilized for handling unstructured and semi-structured data.

### 3. Data Collection for Multiple Channels
- **Collect Data from Up to 10 Channels:** Users can collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.

### 4. Migrating Data to SQL Database
- **Migrate Channel Data:** Users can select a channel name and migrate its data from the data lake to a SQL database (e.g., MySQL or PostgreSQL) as tables for better organization and querying.

### 5. Search and Retrieval from SQL Database
- **Search and Retrieve Data:** Users can search and retrieve data from the SQL database using different search options. This includes joining tables to get channel details and performing specific data queries.

## Approach

### Set Up Streamlit App
The application is built using Streamlit to create a user-friendly interface for interaction.

### Connect to YouTube API
The Google API client library for Python is utilized to make requests to the YouTube API and retrieve channel and video data.

### Store Data in MongoDB
Retrieved data from the YouTube API is stored in a MongoDB database, as it is suitable for handling unstructured and semi-structured data.

### Migrate Data to SQL Database
Data collected for multiple channels is migrated to a SQL database, represented as tables using PostgreSQL for better organization and querying capabilities.

### Query the SQL Data Warehouse
SQL queries, potentially using a Python SQL library which is employed to join tables and retrieve specific data based on user input.

### Display Data in the Streamlit App
The retrieved and analyzed data is presented in the Streamlit app, utilizing Streamlit's data visualization features such as charts and graphs to facilitate data analysis.
