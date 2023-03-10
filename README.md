## Spurs Data Engineer Technical Assessment

Hi! My name is João Pedro and this is my repository with all files and codes that I made. Please install the pip requirements disponible on the following path before run the scripts.
>**nba_sas_assessment/config/requirements.txt** 


- Python Scripts

    You can check the python script on folder: 
    > **nba_sas_assessment/python_files**
- SQL CREATE TABLE scripts

    You can check the SQL codes on folder: 
    > **nba_sas_assessment/sql_files**

- Output .parquet files

    You can check the output by reading the **nba_sas_assessment/processed_zone** folders (contains three folders, one for each parquet) or 
    running the **nba_sas_assessment/python_files/main.py** after clone this repository and replace the variables/urls.

Here is a detailed explanation of my concept when I was facing this challenge:

 - **EXTRACT**

    At the first step, we have two paths that we can follow: insert 'links' of raw data into **urls_list** variable (then the code will download all the links on the raw_data folder).
    The second path is just drag and drop the **.7z** file on raw_data folder and let the **urls_list** variable empty (so we can avoid doing repeated transformations).
    Following the process, the code will extract the **.7z** file(s) and store them into **'raw_data/tmp'** folder. 

    We can also schedule a **'FileSensor'** or **'HttpSensor'** on Airflow with the purpose of check if we have new data into the raw folder or download the JSON from HTTP with HttpSensor.

 - **TRANSFORM**
	
	After the arrival of JSON data, we now can interact with these files and give ‘purpose’ to the data. In the JSON, we have basically four main keys: **’gameid’** (represents the game identifier), **‘gamedate’** (date of the game in format yyyy-mm-dd), ‘events’ representing all the events from the game and their respective **‘moments’** (key nested to events which represents all the moments of the game, with the **x/y/z** location of the ball and players). We also have the **‘visitor’** and **‘home’** keys nested inside the **‘events’** key which represents all **‘players’** and **’teams’** data. All the functions are inside of this python file: **spark_etl.py** 
    A good functionality that PySpark has is the **printSchema()** function, which allows us to see what I exposed previously: the json keys and their values (and the keys again if there are some nested values). This was very useful to build the logic to acess JSON data.
	
    With all these reviews about our raw data, we now are able to extract what is important to us: moments of the game, players and teams data. Our final goal here is: create a **list of lists** where the **nested lists** have to be our dataframe rows. With python loops we are able to enter into every value of every event/moment and retrieve the ball information and players information. We can also access **‘gameid’** and **‘gamedate’** and append this data to our lists. 
    Another important step here is to append the **‘event_id’**, **‘shot_clock’** and **‘game_clock’** data to our lists. We will make this using the **extend()** function from Python (allow us to append multiple elements inside a list).
    Following our path presented previously, we now have to create spark dataframes based on our list-of-lists generated by python loops interacting with JSON raw data. For this, we’ll use the simple **createDataFrame()** function from Spark. This function needs to pass an argument: the schema. We created the schema using the created function **struct_field_create()**. After this step, it is time to finally load our data into Postgres. You can the main file created to call these functions on this link: **main.py** 

    All this block can be connected to Airflow and triggered after the file arrival or Http download.

 - **LOAD**

    With the spark dataframes created, we are able to put our data into Postgres. I’m using the neon.tech provider to run a cloud Postgres RDBMS. Once my database was created, it’s time to create our schemas and tables. I’m working with two schemas: game_facts to store the ball and players movement data and teams_data to store teams and players data. All **‘CREATE TABLE’** scripts are disponible on this folder inside this folder: **nba_sas_assessment/sql_files**
	At the end of the python script, we'll do the ingestion of three tables: game_facts.**ball_and_players_location**, teams_dimensions.**team_data** and teams_dimensions.**players_data.**
	As you can see in the following image, we have the relationship between the tables, and the columns in the bold are possible keys to join data between the tables:

	With the function **append_to_postgres()** we just have to specify our .env file with arguments to connect with our postgres database through the JDBC connector and the function will insert the data in the designed tables created below. We also have the **write_parquet()** function, which write the final table as .parquet on output folder (processed_zone)

    I designed this solution thinking in an automated way to identify new data on the same file or new files on the same link. I suppose that we can connect this code to an API endpoint which will return a JSON response, so we can program a HttpSensor on the Airflow environment to check this API endpoint and return the data. 
