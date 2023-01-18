--drop table teams_dimensions.teams_general_info;

create schema if not exists teams_dimensions;

create table if not exists teams_dimensions.teams_general_info (
	
		 last_name varchar(50)
		,first_name varchar(50)
		,player_id bigint
		,jersey_number varchar(3)
		,position varchar(10)
		,game_id varchar(10)
		,team_id varchar(20)
)

;