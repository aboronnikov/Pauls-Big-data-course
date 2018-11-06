--I consider most popular to be the largest number of people, where people = children + adults.

--In order to run this:
--1) Execute "hive", to get into the hive cli.
--2) Execute "source path/to/this/file", this will run the script.

--Here I write a subquery that calculates the overall number of adults and children that visited each hotel.
--Then I choose 3 hotels where adults + children is max.

use pavel_orekhov;

select hotel_country, ad + ch people_num from 
(
select hotel_country, sum(coalesce(srch_adults_cnt, 0)) ad, sum(coalesce(srch_children_cnt, 0)) ch from train 
where is_booking = 1
group by hotel_country
) y
order by people_num desc limit 3;
