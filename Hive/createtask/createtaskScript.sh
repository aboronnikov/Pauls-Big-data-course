#!/bin/bash
hive --hiveconf dbName=pavel_orekhov \
--hiveconf csvName=../train.csv \
--hiveconf tableName=train \
-f /createtask/create_script.hql

exit $?