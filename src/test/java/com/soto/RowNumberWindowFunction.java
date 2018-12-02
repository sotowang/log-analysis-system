package com.soto;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class RowNumberWindowFunction {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("RowNumberWindowFunction")
                .setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());

        hiveContext.sql("drop table if exists sales");
        hiveContext.sql("create table if not exists sales(" +
                "product string," +
                "category string," +
                "revenue bigint" +
                ")");
        hiveContext.sql("LOAD DATA LOCAL INPATH 'file:///home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/sparkhomework/src/test/java/com/soto/sales.txt' overwrite INTO TABLE sales");

        // 先说明一下，row_number()开窗函数，它的作用是什么？
         // 其实，就是给每个分组的数据，按照其排序顺序，打上一个分组内的行号！！！！
         // 比如说，有一个分组date=20160706，里面看有3数据，11211，11212，11213
         // 那么对这个分组的每一行使用row_number()开窗函数以后，这个三行会打上一个组内的行号！！！
         // 行号是从1开始递增！！！ 比如最后结果就是 11211 1, 11212 2, 11213 3

        DataFrame top3SalesDF = hiveContext.sql("select product, category, revenue "
                +"from (" +
                "select product,category,revenue," +
                "row_number() over (partition by category order by revenue desc) rank " +
                "from sales " +
                ") tmp_sales " +
                "where rank <=3"
        );


//        DataFrame top3SalesDF = hiveContext.sql("SELECT product, category, revenue "
//                + "FROM ("
//             + "SELECT "
//             + "product, "
//             + "category , "
//             + "revenue, "
//             + "row_number() OVER (PARTITION BY category ORDER BY revenue DESC) rank "
//             + "FROM sales "
//             + ") tmp_sales "
//             + "WHERE rank <= 3");

        //将每组前3名的数据保存到一个表中
        hiveContext.sql("drop table if exists top3_sales");
        top3SalesDF.saveAsTable("top3_sales");



        sc.close();
    }
}
