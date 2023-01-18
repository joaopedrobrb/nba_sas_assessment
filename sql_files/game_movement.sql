--drop table game_facts.ball_and_players_location;

create schema if not exists game_facts;

create table if not exists game_facts.ball_and_players_location (

         team_id bigint
        ,player_id bigint
        ,location_x float
        ,location_y float
        ,location_z float
        ,game_clock float
        ,shot_clock float
        ,period int
        ,game_id varchar(10)
        ,event_id varchar (3)
)

;