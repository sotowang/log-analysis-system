package com.soto.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.soto.conf.ConfigurationManager;
import com.soto.constant.Constants;
import com.soto.dao.ITaskDAO;
import com.soto.dao.impl.DAOFactory;
import com.soto.domain.Task;
import com.soto.util.ParamUtils;
import com.soto.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * 各区域top3热门商品统计Spark作业
 */
public class AreaTop3ProducrSpark {

    public static void main(String[] args) {
        // 创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("AreaTop3ProductSpark");
        SparkUtils.setMaster(conf);

        // 构建Spark上下文
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

//        // 注册自定义函数
//        sqlContext.udf().register("concat_long_string",
//                new ConcatLongStringUDF(), DataTypes.StringType);
//        sqlContext.udf().register("get_json_object",
//                new GetJsonObjectUDF(), DataTypes.StringType);
//        sqlContext.udf().register("random_prefix",
//                new RandomPrefixUDF(), DataTypes.StringType);
//        sqlContext.udf().register("remove_random_prefix",
//                new RemoveRandomPrefixUDF(), DataTypes.StringType);
//        sqlContext.udf().register("group_concat_distinct",
//                new GroupConcatDistinctUDAF());


        // 准备模拟数据
        SparkUtils.mockData(sc, sqlContext);


        // 获取命令行传入的taskid，查询对应的任务参数
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        long taskid = ParamUtils.getTaskIdFromArgs(args,
                Constants.SPARK_LOCAL_TASKID_PRODUCT);
        Task task = taskDAO.findById(taskid);

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);


        JavaPairRDD<Long,Row> clickActionRDD = getcityid2ClickActionRDDByDate(sqlContext, startDate, endDate);

        // 从MySQL中查询城市信息
        JavaPairRDD<Long, Row> cityInfoRDD = getcityid2CityInfoRDD(sqlContext);


        sc.close();
    }


    /**
     * 查询指定日期范围内的点击行为数据
     *
     * @param sqlContext
     * @param startDate  起始日期
     * @param endDate    截止日期
     * @return
     */
    private static JavaPairRDD<Long, Row> getcityid2ClickActionRDDByDate(SQLContext sqlContext, String startDate, String endDate) {

        // 从user_visit_action中，查询用户访问行为数据
        // 第一个限定：click_product_id，限定为不为空的访问行为，那么就代表着点击行为
        // 第二个限定：在用户指定的日期范围内的数据
        String sql =
                "SELECT "
                        + "city_id,"
                        + "click_product_id product_id "
                        + "FROM user_visit_action "
                        + "WHERE click_product_id IS NOT NULL "
                        + "AND click_product_id != 'NULL'"
                        + "AND click_product_id != 'null'"
                        + "AND action_time>='" + startDate + "' "
                        + "AND action_time<='" + endDate + "'";
        DataFrame clickActionDF = sqlContext.sql(sql);

        JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();

        JavaPairRDD<Long, Row> cityid2clickActionRDD = clickActionRDD.mapToPair(

                new PairFunction<Row, Long, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        Long cityid = row.getLong(0);
                        return new Tuple2<Long, Row>(cityid, row);
                    }

                });

        return cityid2clickActionRDD;
    }

    /**
     * 使用Spark SQL从MySQL中查询城市信息
     *
     * @param sqlContext
     * @return
     */
    private static JavaPairRDD<Long, Row> getcityid2CityInfoRDD(SQLContext sqlContext) {
        // 构建MySQL连接配置信息（直接从配置文件中获取）
        String url = null;
        String user = null;
        String password = null;
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

        if (local) {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        } else {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
        }

        Map<String, String> options = new HashMap<String, String>();
        options.put("url", url);
        options.put("dbtable", "city_info");
        options.put("user", user);
        options.put("password", password);


        // 通过SQLContext去从MySQL中查询数据
        DataFrame cityInfoDF = sqlContext.read().format("jdbc")
                .options(options).load();

        // 返回RDD
        JavaRDD<Row> cityInfoRDD =  cityInfoDF.javaRDD();


        JavaPairRDD<Long, Row> cityid2cityInfoRDD = cityInfoRDD.mapToPair(

                new PairFunction<Row, Long, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        long cityid = Long.valueOf(String.valueOf(row.get(0)));
                        return new Tuple2<Long, Row>(cityid, row);
                    }

                });

        return cityid2cityInfoRDD;

    }
}
