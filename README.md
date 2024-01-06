# YOUTUBE-DATA-HARVESTING-AND-WAREHOUSING
**Introduction**

The YouTube Data Harvesting and Warehousing project focuses on creating an intuitive Streamlit application utilizing the Google API's capabilities to extract valuable insights from YouTube channels. The obtained data undergoes storage in a MongoDB database and is later migrated to a SQL data warehouse. The Streamlit app facilitates easy analysis and exploration of the stored data, offering a user-friendly interface for users.

**Project Overview**

  The YouTube Data Harvesting and Warehousing project consists of the following components:
  
   •	YouTube API Integration: Integration with the YouTube API to fetch channel and video data based on the provided channel ID.
   
   •	MongoDB: Storage of the retrieved data in a MongoDB database, providing a flexible and scalable solution for storing unstructured and semi-structured data.
      
  •	SQL Data Warehouse: Migration of data from MongoDB to SQL database, allowing for efficient querying and analysis using SQL queries.
  
  •	Streamlit Application: A user-friendly UI built using Streamlit library, allowing users to interact with the application and perform data retrieval and analysis tasks.
    
**Technologies Used**

The following technologies are used in this project:

  •	Python: The programming language used for building the application and scripting tasks.
  
  •	Streamlit: A Python library used for creating interactive web applications and data visualizations.
  
  •	YouTube API: Google API is used to retrieve channel and video data from YouTube.
  
  •	MongoDB: A NoSQL database used as a data lake for storing retrieved YouTube data.
  
  •	SQL (MySQL): A relational database used as a data warehouse for storing migrated YouTube data.
  
  •	PYMYSQL: A Python library used for SQL database connectivity and interaction.
  
  •	Pandas: A data manipulation library used for data processing and analysis.
  
**Installations**

To run the project, need to install the following packages:

   Install Python: Install the Python programming language (I used Jupiter Notebook).
   
   Install Required Libraries: Install the necessary Python libraries using pip package manager. Required libraries include google-api-python-client, 
                                Streamlit, Pandas, pymongo, pymysql and MongoClient.
                                
  Set Up Google API: Set up a Google API project and obtain the necessary API credentials for accessing the YouTube API.
  
  Configure Database: Set up a MongoDB database and SQL database (MySQL) for storing the data.
  
  Configure Application: Update the configuration file or environment variables with the necessary API credentials and database connection details.
  
  Run the Application: Launch the Streamlit application using the command-line interface.

**Usage**

This Project provides a user-friendly interface accessible through a web browser. Below, you'll find a step-by-step guide on how to make the most of the application:

  1.Retrieve Channel Data:
      Enter the YouTube channel ID of interest to fetch detailed information about that specific channel.
      
  2.Store Data in MongoDB:
      Easily store the retrieved data in the MongoDB data lake for flexible and scalable data storage.
      
  3.Migrate to SQL Data Warehouse:
    	Select a channel and seamlessly migrate its data from MongoDB to the SQL data warehouse for advanced querying and analysis.
     
  4.Search and Retrieve Data:
    	Utilize various search options to find and retrieve specific data from the SQL database.
     
  5.Data Analysis and Visualization:
    	Leverage the application's features for in-depth data analysis and visualization to gain valuable insights.

**Conclusion**

In summary, This project is a helpful tool for anyone interested in YouTube data. Whether you're a data enthusiast, content creator, or analyst, this project makes it easy to work with YouTube information. By combining SQL, MongoDB, and Streamlit,I have created a user-friendly and visually pleasing way to interact with and analyze YouTube data. This project opens the door to valuable insights, making it simpler for users to explore and manage the extensive world of YouTube content.



