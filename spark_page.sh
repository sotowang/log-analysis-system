#!/usr/bin/env bash
spark-submit \
--class com.soto.spark.page.PageOneStepConvertRateSpark \
--num-executors 1 \
--driver-memory 100m \
--executor-memory 100m \
--executor-cores 1 \
--files /home/sotowang/user/aur/hadoop/app/hive-1.1.0-cdh5.7.0/conf/hive-site.xml \
--driver-class-path /home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/plugins/maven/repository/mysql/mysql-connector-java/5.1.44/mysql-connector-java-5.1.44.jar \
/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/sparkhomework/out/artifacts/sparkhomework_jar/sparkhomework.jar \
${1}
