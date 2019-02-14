CREATE INDEX ${hiveconf:indexName}
ON TABLE ${hiveconf:tableName} (device)
AS 'BITMAP'
WITH DEFERRED REBUILD;