from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,LongType,IntegerType,FloatType,DateType
from pyspark.sql.functions import *
import pyspark.pandas as pd
import os
from urllib import request
from py7zr import unpack_7zarchive
import shutil
import glob
import json
from dotenv import load_dotenv


def create_spark_session(jar_file_path):
    # create our spark context to create dataframe based on json parsing 
    jar = jar_file_path
    sparkClassPath = os.getenv('SPARK_CLASSPATH', jar)
    spark = (SparkSession.builder.config('spark.jars', f'file:{sparkClassPath}').config('spark.executor.extraClassPath', sparkClassPath).config('spark.driver.extraClassPath', sparkClassPath).appName("PySpark processing NBA data")\
    .getOrCreate())
    
    return spark


def extract_files(urls,folder_raw,folder_tmp_path):

    # create a folder to store our data if not exists
    folder = folder_raw
    path_tmp = folder_tmp_path

    if not os.path.exists(folder):
        os.makedirs(folder)

    if not os.path.exists(folder_tmp_path):
        os.makedirs(folder_tmp_path)


    # download url file from git repo and save into folder created
    counter = 0
    for i in urls:
        url_to_download = urls[counter]
        response_download_name = f'{folder}/{urls[counter].split("/")[-1][:-3]}.7z'
        response = request.urlretrieve(url_to_download, response_download_name)
        counter += 1

    # register unzip 7z function
    try:
        shutil.register_unpack_format('7zip', ['.7z'], unpack_7zarchive)
    except Exception:
        pass

    # list all files from download folder
    file_name = glob.glob(f'{folder}/*.7z')

    # create a count
    c = 0

    # path to output zipped files
    output_path = f'{folder}/tmp/'

    # for each file from download folder, decompress zipped and save into /tmp folder
    for i in file_name:
        shutil.unpack_archive(file_name[c], output_path)
        os.remove(file_name[c])
        c += 1

    # path where json is stored
    path = folder_tmp_path

    # listing all files inside the path
    folder = os.listdir(path)
    # create a empty list to append if we have json files inside the folder
    game_files = []
    c = 0
    # for each information in folder
    for i in folder:
        # if we have '.json' in string, append to our previous empty list
        if '.json' in folder[c]:
            game_files.append(folder[c])
        c =+ 1

    return game_files
    

def get_teams_data(game_files,folder_tmp_path):

    # use python open method to open json file on read mode
    game_file = open(f'{folder_tmp_path}/{game_files}', 'r')
    # using json loads to load the json data
    data = json.load(game_file)
    # catching up the key 'events' so we can go inside of the key values 
    events = data['events'][0]
    teams = []
    teams.extend((events['home']['name'],events['home']['abbreviation'],events['home']['teamid'],events['visitor']['name'],events['visitor']['abbreviation'],events['visitor']['teamid'],data['gameid'],data['gamedate']))

    return [teams]


def get_players_dimension_home(game_files,folder_tmp_path):

    c_home = 0
    # path where json is stored
    path = folder_tmp_path
    # use python open method to open json file on read mode
    game_file = open(f'{path}/{game_files}', 'r')
    # using json loads to load the json data
    data = json.load(game_file)
    data_home = data['events']
    # HOME TEAM INFO #
    player_data_home = []
    # fetching players of home team 
    players_query = data_home[0]['home']['players']
    for i in players_query:
        player_data_home.append([i for i in players_query[c_home].values()])
        player_data_home[-1].extend((data['gameid'],data_home[c_home]['home']['teamid'],data['gamedate']))
        c_home += 1

    return player_data_home

def get_players_dimension_visitant(game_files,folder_tmp_path):

    c_visitant = 0
    # path where json is stored
    path = folder_tmp_path
    # use python open method to open json file on read mode
    game_file = open(f'{path}/{game_files}', 'r')
    # using json loads to load the json data
    data = json.load(game_file)
    data_home = data['events']
    # VISITANT TEAM INFO #
    player_data_visitant = []
    data_visitant = data['events']
    players_query = data_visitant[0]['visitor']['players']
    for i in players_query:
        player_data_visitant.append([i for i in players_query[c_visitant].values()])
        player_data_visitant[-1].extend((data['gameid'],data_visitant[c_visitant]['visitor']['teamid'],data['gamedate']))
        c_visitant += 1    
    c =+ 1

    return player_data_visitant


def get_location_of_ball(game_files,folder_tmp_path):
    # use python open method to open json file on read mode
    game_file = open(f'{folder_tmp_path}/{game_files}', 'r')
    # using json loads to load the json data
    data = json.load(game_file)
    # catching up the key 'events' so we can go inside of the key values 
    events = data['events']
    # create a list to store the moments keys (location, ball, player, team id)
    location_data = []
    # for each play in events, we are able to store the eventId and also the moments (with ball/team location info)
    for play in events:
        # store the eventid
        event_id = play['eventId']
        # store the court info 
        court_info = play['moments']
        # for each location info on court
        for location in court_info:
            # select the value where ball and player location are stored
            for ball_or_player in location[5]:
                # 'extend' allow create a list with multiple info, so we can load this into spark dataframe (horizontal data)
                ball_or_player.extend((location[2], location[3], location[0], data['gameid'], event_id, data['gamedate']))
                location_data.append(ball_or_player)
        # break

    return location_data



def struct_field_create():
    
    # create our schema to upload data
    schema_location_of_ball_and_teams = StructType([ \
        StructField("team_id",LongType(),True),
        StructField("player_id",LongType(),True),
        StructField("location_x",FloatType(),True),
        StructField("location_y",FloatType(),True),
        StructField("location_z", FloatType(),True),
        StructField("game_clock", FloatType(),True),
        StructField("shot_clock", FloatType(),True),
        StructField("period", IntegerType(),True),
        StructField("game_id", StringType(),True),
        StructField("event_id", StringType(),True),
        StructField("game_date", StringType(),True)
        ]
    )

    schema_players_info = StructType([ \

        StructField("last_name",StringType(),True),
        StructField("first_name",StringType(),True),
        StructField("player_id",LongType(),True),
        StructField("jersey_number",StringType(),True),
        StructField("position", StringType(),True),
        StructField("game_id", StringType(),True),
        StructField("team_id", StringType(),True),
        StructField("game_date", StringType(),True)
        ]
    )

    schema_teams_info = StructType([ \
        StructField("home_team_name",StringType(),True),
        StructField("home_team_abbreviation",StringType(),True),
        StructField("home_team_id",LongType(),True),
        StructField("visitant_team_name",StringType(),True),
        StructField("visitant_team_abbreviation", StringType(),True),
        StructField("visitant_team_id", LongType(),True),
        StructField("game_id", StringType(),True),
        StructField("game_date", StringType(),True)
    
        ]
    )


    return [schema_location_of_ball_and_teams, schema_players_info, schema_teams_info]


def team_data_df(spark,data_to_process,struct_spark):
    # create the team info (name, abbreviation, id) dimension
    team_info_data = spark.createDataFrame(data=data_to_process,schema=struct_spark[2]) 

    return team_info_data

def home_team_df(spark,data_to_process,struct_spark):
    # create the home team players dimension
    home_team_data = spark.createDataFrame(data=data_to_process,schema=struct_spark[1]) 

    return home_team_data

def visitant_team_df(spark,data_to_process,struct_spark):

    # create the visitant team players dimension
    visitant_team_data = spark.createDataFrame(data=data_to_process,schema=struct_spark[1]) 

    return visitant_team_data

def location_of_the_ball_df(spark,data_to_process,struct_spark):

    # create the ball movement dataframe
    location_of_the_ball = spark.createDataFrame(data=data_to_process,schema=struct_spark[0]) 
    # location_of_the_ball = location_of_the_ball.withColumn('event_id', col('event_id').cast(LongType()))

    return location_of_the_ball


def append_to_postgres(location_data,home_data,visitant_data,team_data,jar):

    location_data = location_data.withColumn('game_date', col('game_date').cast("date"))

    home_data = home_data.withColumn('game_date', col('game_date').cast("date"))

    visitant_data = visitant_data.withColumn('game_date', col('game_date').cast("date"))

    team_data = team_data.withColumn('game_date', col('game_date').cast("date"))

    load_dotenv('./workspace/nba_sas_assessment/config/postgres_login.env')
    URL = "jdbc:postgresql://ep-rapid-cloud-796936.us-east-2.aws.neon.tech/neondb?user=joaopedro.brb&password=ymFheQfG70XC"
    USER = "joaopedro.brb"
    PASS = "ymFheQfG70XC"
    spark = create_spark_session(jar)

    neon_team_data = spark.read.format("jdbc") \
    .option("url", URL) \
    .option("dbtable", "teams_dimensions.team_data") \
    .option("user", USER) \
    .option("password", PASS) \
    .option("driver", "org.postgresql.Driver") \
    .load()

    team_data = neon_team_data.union(team_data)

    team_data.write.format("jdbc").mode('append').option("url", URL)\
    .option("user", USER)\
    .option("password", PASS)\
    .option("dbtable", 'teams_dimensions.team_data')\
    .option("driver", "org.postgresql.Driver")\
    .save()

    # neon_location_data = spark.read.format("jdbc") \
    # .option("url", URL) \
    # .option("dbtable", "game_facts.ball_and_players_location") \
    # .option("user", USER) \
    # .option("password", PASS) \
    # .option("driver", "org.postgresql.Driver") \
    # .load()

    # location_data = neon_location_data.union(location_data)

    location_data.write.format("jdbc").mode('append').option("url", URL)\
    .option("user", USER)\
    .option("password", PASS)\
    .option("dbtable", 'game_facts.ball_and_players_location')\
    .option("driver", "org.postgresql.Driver")\
    .save()

    neon_players_data = spark.read.format("jdbc") \
    .option("url", URL) \
    .option("dbtable", "teams_dimensions.players_data") \
    .option("user", USER) \
    .option("password", PASS) \
    .option("driver", "org.postgresql.Driver") \
    .load()
    all_players_on_game = home_data.union(visitant_data).distinct()
    all_players_on_game = neon_players_data.union(all_players_on_game)

    all_players_on_game.write.format("jdbc").mode('append').option("url", URL)\
        .option("user", USER)\
        .option("password", PASS)\
        .option("dbtable", 'teams_dimensions.players_data')\
        .option("driver", "org.postgresql.Driver")\
        .save()

    # return print(f'{game_files} inserted on table')

def remove_json_files(path,game_file):
    os.remove(f'{path}/{game_file}')

    return print(f'{path}/{game_file} deleted')


def write_parquet(location_data,home_data,visitant_data,team_data,path):
    
    location_data = location_data.withColumn('game_date', col('game_date').cast("date"))

    home_data = home_data.withColumn('game_date', col('game_date').cast("date"))

    visitant_data = visitant_data.withColumn('game_date', col('game_date').cast("date"))

    team_data = team_data.withColumn('game_date', col('game_date').cast("date"))
    
    location_data.write.mode('append').parquet(f'{path}/location_data')

    all_players_on_game = home_data.union(visitant_data).distinct()

    all_players_on_game.write.mode('append').parquet(f'{path}/players_data')

    team_data.write.mode('append').parquet(f'{path}/team_data')

    return print('Output saved')