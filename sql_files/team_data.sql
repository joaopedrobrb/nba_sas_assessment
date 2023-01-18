--drop table teams_dimensions.team_data 

create schema if not exists teams_data;

create table if not exists teams_data.team_data (
	
		 home_team_name varchar(50)
		,home_team_abbreviation varchar(3)
		,home_team_id bigint
		,visitant_team_name varchar(50)
		,visitant_team_abbreviation varchar(3)
		,visitant_team_id bigint
		,game_id varchar(10)
)

;