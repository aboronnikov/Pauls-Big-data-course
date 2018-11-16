#!/bin/bash
hive --hiveconf dbName=pavel_orekhov \
--hiveconf tableName=train \
-f /Task2/task2.hql

exit $?