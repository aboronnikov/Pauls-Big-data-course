#!/usr/bin/env bash
$SPARK_HOME/bin/spark-submit
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2
--master yarn-client
--driver-memory 1G
--num-executors 4
--executor-memory 1G
/home/maria_dev/TopicIngester.jar
-topic topicX
-bootstrapServer sandbox-hdp.hortonworks.com:6667
-format csv
-path /user/maria_dev/test
-startingOffsets \{\"topicX\":\{\"0\":0,\"1\":0\}\}
-endingOffsets \{\"topicX\":\{\"0\":1,\"1\":0\}\}