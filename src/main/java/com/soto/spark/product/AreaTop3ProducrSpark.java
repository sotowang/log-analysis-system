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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

        // 注册自定义函数
        sqlContext.udf().register("concat_long_string",
                new ConcatLongStringUDF(), DataTypes.StringType);
//        sqlContext.udf().register("get_json_object",
//                new GetJsonObjectUDF(), DataTypes.StringType);
//        sqlContext.udf().register("random_prefix",
//                new RandomPrefixUDF(), DataTypes.StringType);
//        sqlContext.udf().register("remove_random_prefix",
//                new RemoveRandomPrefixUDF(), DataTypes.StringType);
        sqlContext.udf().register("group_concat_distinct",
                new GroupConcatDistinctUDAF());


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


        // 查询用户指定日期范围内的点击行为数据（city_id，在哪个城市发生的点击行为）
        // 技术点1：Hive数据源的使用
        JavaPairRDD<Long,Row> cityid2clickActionRDD = getcityid2ClickActionRDDByDate(sqlContext, startDate, endDate);

        // 从MySQL中查询城市信息
        // 技术点2：异构数据源之MySQL的使用
        JavaPairRDD<Long, Row> cityid2cityInfoRDD = getcityid2CityInfoRDD(sqlContext);


        // 生成点击商品基础信息临时表
        // 技术点3：将RDD转换为DataFrame，并注册临时表
        generateTempClickProductBasicTable(sqlContext, cityid2clickActionRDD, cityid2cityInfoRDD);


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

    /**
     * 生成点击商品基础信息临时表
     * @param sqlContext
     * @param cityid2clickActionRDD
     * @param cityid2cityInfoRDD
     */
    private static void generateTempClickProductBasicTable(
            SQLContext sqlContext,
            JavaPairRDD<Long, Row> cityid2clickActionRDD,
            JavaPairRDD<Long, Row> cityid2cityInfoRDD) {


        // 执行join操作，进行点击行为数据和城市数据的关联
        JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD =
                cityid2clickActionRDD.join(cityid2cityInfoRDD);



        // 将上面的JavaPairRDD，转换成一个JavaRDD<Row>（才能将RDD转换为DataFrame）
        JavaRDD<Row> mappedRDD = joinedRDD.map(

                new Function<Tuple2<Long,Tuple2<Row,Row>>, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple)
                            throws Exception {
                        long cityid = tuple._1;
                        Row clickAction = tuple._2._1;
                        Row cityInfo = tuple._2._2;

                        long productid = clickAction.getLong(1);
                        String cityName = cityInfo.getString(1);
                        String area = cityInfo.getString(2);

                        return RowFactory.create(cityid, cityName, area, productid);
                    }

                });


        // 基于JavaRDD<Row>的格式，就可以将其转换为DataFrame
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));


        // 1 北京
        // 2 上海
        // 1 北京
        // group by area,product_id
        // 1:北京,2:上海

        // 两个函数
        // UDF：concat2()，将两个字段拼接起来，用指定的分隔符
        // UDAF：group_concat_distinct()，将一个分组中的多个字段值，用逗号拼接起来，同时进行去重

        StructType schema = DataTypes.createStructType(structFields);

        DataFrame df = sqlContext.createDataFrame(mappedRDD, schema);
        System.out.println("tmp_click_product_basic: " + df.count());

        // 将DataFrame中的数据，注册成临时表（tmp_click_product_basic）
        df.registerTempTable("tmp_click_product_basic");


    }
}
