--Here I consider unique triples <hotel_continent, hotel_country, hotel_market> and count the number of repetitions of each unique triple.

--In order to run this:
--1) Execute "hive", to get into the hive cli.
--2) Execute "source path/to/this/file", this will run the script. 

use pavel_orekhov;

select hotel_continent, hotel_country, hotel_market, count(*) hit from train
where is_booking = 0
group by hotel_continent, hotel_country, hotel_market
order by hit desc
limit 3;