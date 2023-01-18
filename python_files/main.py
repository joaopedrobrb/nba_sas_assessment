from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,LongType,IntegerType,FloatType,DateType
from pyspark.sql.functions import *
import pandas as pd
import pyspark.pandas as ps
import os
from urllib import request
from py7zr import unpack_7zarchive
import shutil
import glob
import json
from script_etl import * 
from dotenv import load_dotenv

## WARNING: BEFORE RUNNING, PLEASE INSTALL THE FILE config/requirements.txt with command: pip install -r config/requirements.txt 

## Replace the next variables with your directory of preference ##

folder_raw = '/workspace/nba_sas_assessment/raw_data' 
folder_tmp_path = "/workspace/nba_sas_assessment/raw_data/tmp" # store json temp
output_parquet_path = "/workspace/nba_sas_assessment/processed_files"

## Replace the next variables with the path to postgres jar file and .env with credencials to access neon.tech ##
jar_postgres_path = '/workspace/nba_sas_assessment/config/jar/postgresql-42.5.1.jar'
env_postgres_credentials = './workspace/nba_sas_assessment/config/postgres_login.env'

# create spark session and return the session
spark = create_spark_session(jar_file_path=jar_postgres_path)

# create spark structure to received processed json data
struct_spark = struct_field_create()

## Here you have two options:
        # 1) If you already had uploaded the file (.7z) inside the 'folder_raw', you can ignore the next variable containing a list
        # 2) If you want to download one or more files and store into the 'folder_raw', fill the list 'urls_list' with the desired links to download

urls_list = ['https://github.com/sealneaward/nba-movement-data/raw/master/data/01.01.2016.CHA.at.TOR.7z']


# Extract the game files from raw
game_files = extract_files(folder_raw=folder_raw,folder_tmp_path=folder_tmp_path,urls=urls_list)     

# for each item on folder_tmp_path (each json file)
for i in range(0,len(game_files)):
    # Get data from teams
    get_teams_data_dim = get_teams_data(game_files=game_files[i],folder_tmp_path=folder_tmp_path)                                                                                                                                                                                                                                       
    # Get data from visitant team (players)
    get_visitant_players_info = get_players_dimension_visitant(game_files=game_files[i],folder_tmp_path=folder_tmp_path)
    # Get data from home team (players)
    get_home_players_info = get_players_dimension_home(game_files=game_files[i],folder_tmp_path=folder_tmp_path)
    # Get ball movement data
    get_location_data = get_location_of_ball(game_files=game_files[i],folder_tmp_path=folder_tmp_path)
    # Transform team data into dataframe
    team_data = team_data_df(spark=spark,data_to_process=get_teams_data_dim,struct_spark=struct_spark)
    # Transform visitant team (players) into dataframe
    visitant_team = visitant_team_df(spark=spark,struct_spark=struct_spark,data_to_process=get_visitant_players_info)
    # Transform home team (players) into dataframe
    home_team = home_team_df(spark=spark,data_to_process=get_home_players_info,struct_spark=struct_spark)
    # Transform ball movement into dataframe
    location_of_the_ball = location_of_the_ball_df(spark=spark,data_to_process=get_location_data,struct_spark=struct_spark)
        ## TWO OPTIONS:
                # 1) Write parquet output on 'output_parquet_path' folder
                # 2) Write into neon.tech database throught 'append_to_postgres' function 
                # With all these following codes, I'm doing both of the options =)
    write_parquet = write_parquet(location_data=location_of_the_ball,home_data=home_team,visitant_data=visitant_team,team_data=team_data,path=output_parquet_path)

    # Append all these dataframes into our neon.tech database
    append_to_postgres(location_data=location_of_the_ball\
                       ,home_data=home_team\
                       ,visitant_data=visitant_team\
                       ,team_data=team_data\
                       ,jar=jar_postgres_path\
                       )

    # Remove JSON files to clean the folder
    remove_json_files(path=folder_tmp_path,game_file=game_files[i])

