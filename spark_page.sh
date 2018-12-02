#!/usr/bin/env bash
spark-submit \
--class com.soto.spark.product.AreaTop3ProductSpark \
--num-executors 1 \
--driver-memory 1g \
--executor-memory 100m \
--executor-cores 1 \
--jars /home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/plugins/maven/repository/org/apache/spark/spark-hive_2.10/1.6.0-cdh5.7.0/spark-hive_2.10-1.6.0-cdh5.7.0.jar \
--files /home/sotowang/user/aur/hadoop/app/hive-1.1.0-cdh5.7.0/conf/hive-site.xml \
--driver-class-path /home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/plugins/maven/repository/mysql/mysql-connector-java/5.1.44/mysql-connector-java-5.1.44.jar \
/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/sparkhomework/target/spark-homework-1.0-jar-with-dependencies.jar \
2
