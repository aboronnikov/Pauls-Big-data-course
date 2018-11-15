--I consider a couple to be equal to 2 adults.

--In order to run this:
--1) Execute "hive", to get into the hive cli.
--2) Execute "source path/to/this/file", this will run the script. 

use pavel_orekhov;

select max(datediff(to_date(srch_co), to_date(srch_ci))) stay  from train 
where srch_children_cnt > 0 and srch_adults_cnt = 2 and not(srch_co is  null) and not(srch_ci is null);