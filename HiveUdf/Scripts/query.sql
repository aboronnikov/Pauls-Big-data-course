-- First we create a CTE to join browserdata on citydata to get proper citynames.
WITH
t1 as
(select * from ${hiveconf:browserTable} join ${hiveconf:cityTable} on cityid=id),

-- This CTE parses the user agent string into multiple fields.
t2 as
(
	select uap.device as device, uap.os as os, uap.browser as browser, name as cityname
	from t1
	lateral view ParseUserAgentUDTF(UserAgent) uap as device, os, browser
),

-- This CTE for each composite key <cityname, os, device browser> calculates its number of occurences in the table.
t3 as
(
	select t2.cityname as cityname, t2.device as device, t2.browser as browser, t2.os as os, count(*) as count
	from t2
	group by t2.cityname, t2.os, t2.device, t2.browser
)

-- This query uses analytic functions to find <cityname, os, device browser, count> for each city, such that count is maximal.
select cityname, maximum,  device, os, browser
from
     (
        select cityname, device, browser, os,
             max(count) over(partition by cityname)                         as maximum,
             dense_rank() over (partition by cityname order by count desc ) as rnk
        from t3
     ) s  where rnk = 1
;