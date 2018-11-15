#!/bin/bash
hive --hiveconf dbName=pavel_orekhov \
--hiveconf tableName=train \
-f task2.hql

exit $?