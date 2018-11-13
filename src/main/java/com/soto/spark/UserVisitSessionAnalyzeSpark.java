package com.soto.spark;

import com.alibaba.fastjson.JSONObject;
import com.soto.conf.ConfigurationManager;
import com.soto.constant.Constants;
import com.soto.dao.ITaskDAO;
import com.soto.dao.impl.DAOFactory;
import com.soto.domain.Task;
import com.soto.test.MockData;
import com.soto.util.DateUtils;
import com.soto.util.ParamUtils;
import com.soto.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Iterator;

/**
 * 用户访问session分析Spark作业
 *
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 *  *
 *  * 1、时间范围：起始日期~结束日期
 *  * 2、性别：男或女
 *  * 3、年龄范围
 *  * 4、职业：多选
 *  * 5、城市：多选
 *  * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 *  * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 *
 *
 *
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION)
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = getSQLContext(sc.sc());

        //生成模拟测试数据
        mockData(sc,sqlContext);

        // 创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();


        long taskid = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDAO.findById(taskid);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //如果要进行session粒度的数据聚合
        //首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);

        //首先，可以将行为数据，按照session_id 进行groupByKey分组
        //此时的数据粒度就是session粒度了，然后可以将session粒度的数据与用户信息数据进行join
        //然后就可以获取到session粒度的数据，購，数据里还包含了session对应的user信息

        // 到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggrateBySession(sqlContext, actionRDD);
        //关闭spark上下文
        sc.close();

    }

    /**
     * 生成模拟数据（只有本地模式，才会去生成模拟数据）
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local) {
            MockData.mock(sc, sqlContext);
        }
    }

    /**
     * 获取指定日期范围内的用户访问行为数据
     * @param sqlContext SQLContext
     * @param taskParam 任务参数
     * @return 行为数据RDD
     */
    private static JavaRDD<Row> getActionRDDByDateRange(
            SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        String sql =
                "select * "
                        + "from user_visit_action "
                        + "where date>='" + startDate + "' "
                        + "and date<='" + endDate + "'";
//				+ "and session_id not in('','','')"

        DataFrame actionDF = sqlContext.sql(sql);

        /**
         * 这里就很有可能发生上面说的问题
         * 比如说，Spark SQl默认就给第一个stage设置了20个task，但是根据你的数据量以及算法的复杂度
         * 实际上，你需要1000个task去并行执行
         *
         * 所以说，在这里，就可以对Spark SQL刚刚查询出来的RDD执行repartition重分区操作
         */

//		return actionDF.javaRDD().repartition(1000);

        return actionDF.javaRDD();
    }


    /**
     * 对行为数据按照session粒度聚合
     * @param actionRDD  行为数据RDD
     * @return  session粒度聚合数据
     */
    private static JavaPairRDD<String, String> aggrateBySession(SQLContext sqlContext, JavaRDD<Row> actionRDD) {

        //现在actionRDD中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击或搜索
        //现在需要将这个Row映射成<sessionid,Row>格式
        JavaPairRDD<String, Row> sessionid2ActionRDD =  actionRDD.mapToPair(
                /**
                 * PairFunction
                 * 第一个参数，相当于函数的输入
                 * 第二个参数和第三个参数，相当于函数的输出（Tuple），分别是Tuple第一个和第二个值
                 */
                new PairFunction<Row, String, Row>() {

                    private static final long serialVersionUID = 1L;


                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        return new Tuple2<String,Row>(row.getString(2),row);
                    }
                }
        );

        //对行为数据按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD =
                sessionid2ActionRDD.groupByKey();

        //对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        // 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long,String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(

                new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        String sessionid = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();


                        StringBuffer searchKeywordBuffer = new StringBuffer("");
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                        Long userid = null;


                        //遍历session所有的访问行为
                        while (iterator.hasNext()) {
                            //提取每个访问行为的搜索字段和点击品类字段
                            Row row = iterator.next();
                            if (userid == null) {
                                userid = row.getLong(1);
                            }
                            String searchKeyword = row.getString(5);
                            long clickCategoryId = row.getLong(6);

                            //实际上并不是每一行访问行为都有searchKeyword和clickCategory字段
                            //任何一行行为数据都不可能两个字段都有，可能出现null
                            //将搜索词或点击品类id拼接到字符串中去
                            //首先要满足：不能是null值
                            //其次，之前的字符串中还没有搜索词或都点击品类

                            if (StringUtils.isNotEmpty(searchKeyword)) {
                                if (!searchKeywordBuffer.toString().contains(searchKeyword)) {
                                    searchKeywordBuffer.append(searchKeyword + ",");
                                }
                            }
                            if (clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
                                clickCategoryIdsBuffer.append(clickCategoryId + ",");
                            }
                        }

                        String searchKeywords = StringUtils.trimComma(searchKeywordBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        // 返回的数据格式，即<sessionid,partAggrInfo>
                        // 如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
                        // 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
                        // 这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
                        // 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举

                        // 所以这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
                        // 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
                        // 然后再直接将返回的Tuple的key设置成sessionid
                        // 最后的数据格式，还是<sessionid,fullAggrInfo>

                        // 聚合数据，用什么样的格式进行拼接？
                        // 这里统一定义，使用key=value|key=value

                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                                + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;
                        return new Tuple2<Long, String>(userid,partAggrInfo);
                    }
                });

        //查询所有用户数据,并映射成<userid，Row>格式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        return new Tuple2<Long, Row>(row.getLong(0), row);
                    }
                }
        );

        //将session粒度聚合数据，与用户信息数据进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);

        //对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo> 格式的数据
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(

                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
                    private static final long serialVersionUID = 1L;


                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                        String partAggrInfo = tuple._2._1;
                        Row userInfoRow = tuple._2._2;

                        String sessionid = StringUtils.getFieldFromConcatString(
                                partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                        int age = userInfoRow.getInt(3);
                        String professional = userInfoRow.getString(4);
                        String city = userInfoRow.getString(5);
                        String sex = userInfoRow.getString(6);

                        String fullAggrInfo = partAggrInfo + "|"
                                + Constants.FIELD_AGE + "=" + age + "|"
                                + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                                + Constants.FIELD_CITY + "=" + city + "|"
                                + Constants.FIELD_SEX + "=" + sex;


                        return new Tuple2<String, String>(sessionid, fullAggrInfo);
                    }
                }
        );



        return sessionid2FullAggrInfoRDD;

    }

    /**
     * 获取SQLContext
     * 如果是在本地测试环境，生成SQLContext对象
     * 如果是在生产环境，生成HiveContext对象
     * @param sc
     * @return
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }
}
