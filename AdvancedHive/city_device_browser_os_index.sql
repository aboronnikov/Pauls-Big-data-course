CREATE INDEX ${hiveconf:index_name}
ON TABLE ${hiveconf:tableName} (cityid,device,browser,os)
AS 'COMPACT'
WITH DEFERRED REBUILD;