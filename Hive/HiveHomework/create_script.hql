--In order to run this:
--0) Make sure test.csv is in path.
--1) Execute "hive", to get into the hive cli.
--2) Execute "source path/to/this/file", this will run the script. 

create database if not exists pavel_orekhov;

create external table if not exists pavel_orekhov.train
(
        id string,
        date_time string,
        site_name int,
        posa_continent int,
        user_location_country int,
        user_location_region int,
        user_location_city int,
        orig_destination_distance double,
        user_id int,
        is_mobile int,
        is_package int,
        channel int,
        srch_ci string,
        srch_co string,
        srch_adults_cnt int,
        srch_children_cnt int,
        srch_rm_cnt int,
        srch_destination_id int,
        srch_destination_type_id int,
        hotel_continent int,
        hotel_country int,
        hotel_market int,
        is_booking int,
        cnt bigint,
        hotel_cluster int
)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile;

use pavel_orekhov;
load data local inpath "test.csv" into table train;
