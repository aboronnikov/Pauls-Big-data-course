#!/bin/bash
hive --hiveconf dbName=pavel_orekhov \
--hiveconf csvName=train.csv \
--hiveconf tableName=train \
-f create_script.hql

exit $?