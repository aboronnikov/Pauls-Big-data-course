CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:tableName} (
	id int,
	name string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ${hiveconf:delimiter}
STORED AS TEXTFILE
LOCATION ${hiveconf:pathToData};