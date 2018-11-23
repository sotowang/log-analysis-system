package com.soto.spark.session;

import com.google.common.base.Optional;
import com.alibaba.fastjson.JSONObject;
import com.soto.conf.ConfigurationManager;
import com.soto.constant.Constants;
import com.soto.dao.*;
import com.soto.dao.impl.DAOFactory;
import com.soto.domain.*;
import com.soto.test.MockData;
import com.soto.util.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 * 用户访问session分析Spark作业
 * <p>
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * *
 * * 1、时间范围：起始日期~结束日期
 * * 2、性别：男或女
 * * 3、年龄范围
 * * 4、职业：多选
 * * 5、城市：多选
 * * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {

        args = new String[]{"1"};
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                .set("spark.storage.memoryFraction", "0.5")
                .set("spark.shuffle.consolidateFiles", "true")
                .set("spark.shuffle.file.buffer", "64")
                .set("spark.shuffle.memoryFraction", "0.3")
                .set("spark.reducer.maxSizeInFlight", "24")
                .setMaster("local")
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{
                        CategorySortKey.class,
                        IntList.class
                });

        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = getSQLContext(sc.sc());

        //生成模拟测试数据
        mockData(sc, sqlContext);

        // 创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();


        long taskid = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDAO.findById(taskid);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //如果要进行session粒度的数据聚合
        //首先要从user_visit_action表中，查询出来指定日期范围内的行为数据

        /**
         * actionRDD，就是一个公共RDD
         * 第一，要用ationRDD，获取到一个公共的sessionid为key的PairRDD
         * 第二，actionRDD，用在了session聚合环节里面
         *
         * sessionid为key的PairRDD，是确定了，在后面要多次使用的
         * 1、与通过筛选的sessionid进行join，获取通过筛选的session的明细数据
         * 2、将这个RDD，直接传入aggregateBySession方法，进行session聚合统计
         *
         * 重构完以后，actionRDD，就只在最开始，使用一次，用来生成以sessionid为key的RDD
         *
         */
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);


        JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);

        /**
         * RDD持久化
         * 如果是persist(StorageLevel.MEMORY_ONLY())，纯内存，无序列化，那么就可以用cache()方法来替代
         * 	StorageLevel.MEMORY_ONLY_SER()，第二选择
         *  StorageLevel.MEMORY_AND_DISK()，第三选择
         *  StorageLevel.MEMORY_AND_DISK_SER()，第四选择
         *  StorageLevel.DISK_ONLY()，第五选择
         *
         *  如果内存充足，要使用双副本高可靠机制
         *  选择后缀带_2的策略
         *  StorageLevel.MEMORY_ONLY_2()
         */
        sessionid2actionRDD = sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY());



//        System.out.println("actionRDD+============"+actionRDD.count());
        //首先，可以将行为数据，按照session_id 进行groupByKey分组
        //此时的数据粒度就是session粒度了，然后可以将session粒度的数据与用户信息数据进行join
        //然后就可以获取到session粒度的数据，購，数据里还包含了session对应的user信息

        // 到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggrateBySession(sqlContext, sessionid2actionRDD);


        // 重构，同时进行过滤和统计
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator(
                "", new SessionAggrStatAccumulator());


        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
                sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);


        filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());


        // 生成公共的RDD：通过筛选条件的session的访问明细数据

        /**
         * 重构：sessionid2detailRDD，就是代表了通过筛选的session对应的访问明细数据
         */
        JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(
                filteredSessionid2AggrInfoRDD, sessionid2actionRDD);

        sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());


        randomExtractSession(sc, task.getTaskid(), filteredSessionid2AggrInfoRDD, sessionid2actionRDD);

        /**
         * 对于Accumulator这种分布式累加计算的变量的使用，有一个重要说明
         *
         * 从Accumulator中，获取数据，插入数据库的时候，一定要，一定要，是在有某一个action操作以后
         * 再进行。。。
         *
         * 如果没有action的话，那么整个程序根本不会运行。。。
         *
         * 是不是在calculateAndPersisitAggrStat方法之后，运行一个action操作，比如count、take
         * 不对！！！
         *
         * 必须把能够触发job执行的操作，放在最终写入MySQL方法之前
         *
         * 计算出来的结果，在J2EE中，是怎么显示的，是用两张柱状图显示
         */


        System.out.println("filteredSessionid2AggrInfoRDD================" + filteredSessionid2AggrInfoRDD.count());

        // 计算出各个范围的session占比，并写入MySQL
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),
                task.getTaskid());

        // 获取top10热门品类
        List<Tuple2<CategorySortKey, String>> top10CategoryList = getTop10Category(task.getTaskid(), sessionid2detailRDD);


        // 获取top10活跃session
        getTop10Session(sc, task.getTaskid(), top10CategoryList, sessionid2detailRDD);


        //关闭spark上下文
        sc.close();

    }


    /**
     * 生成模拟数据（只有本地模式，才会去生成模拟数据）
     *
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }

    /**
     * 获取指定日期范围内的用户访问行为数据
     *
     * @param sqlContext SQLContext
     * @param taskParam  任务参数
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
         *
         * 比如说，Spark SQl默认就给第一个stage设置了20个task，但是根据数据量以及算法的复杂度
         * 实际上，需要1000个task去并行执行
         *
         * 在这里，就可以对Spark SQL刚刚查询出来的RDD执行repartition重分区操作
         */

//		return actionDF.javaRDD().repartition(1000);

        return actionDF.javaRDD();
    }


    private static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
//        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
//
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Tuple2<String, Row> call(Row row) throws Exception {
//                return new Tuple2<String, Row>(row.getString(2), row);
//            }
//
//        });

        return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterable<Tuple2<String, Row>> call(Iterator<Row> iterator)
                    throws Exception {
                List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();

                while(iterator.hasNext()) {
                    Row row = iterator.next();
                    list.add(new Tuple2<String, Row>(row.getString(2), row));
                }

                return list;
            }

        });

    }


    /**
     * 对行为数据按照session粒度聚合
     *
     * @param sessionid2actionRDD 行为数据RDD
     * @return session粒度聚合数据
     */
    private static JavaPairRDD<String, String> aggrateBySession(SQLContext sqlContext, JavaPairRDD<String,Row> sessionid2actionRDD) {

//        //现在actionRDD中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击或搜索
//        //现在需要将这个Row映射成<sessionid,Row>格式
//        JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(
//                /**
//                 * PairFunction
//                 * 第一个参数，相当于函数的输入
//                 * 第二个参数和第三个参数，相当于函数的输出（Tuple），分别是Tuple第一个和第二个值
//                 */
//                new PairFunction<Row, String, Row>() {
//
//                    private static final long serialVersionUID = 1L;
//
//
//                    @Override
//                    public Tuple2<String, Row> call(Row row) throws Exception {
//                        return new Tuple2<String, Row>(row.getString(2), row);
//                    }
//                }
//        );


        //对行为数据按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD =
                sessionid2actionRDD.groupByKey();

        //对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        // 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(

                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple)
                            throws Exception {
                        String sessionid = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        StringBuffer searchKeywordsBuffer = new StringBuffer("");
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                        Long userid = null;

                        // session的起始和结束时间
                        Date startTime = null;
                        Date endTime = null;
                        // session的访问步长
                        int stepLength = 0;

                        // 遍历session所有的访问行为
                        while (iterator.hasNext()) {
                            // 提取每个访问行为的搜索词字段和点击品类字段
                            Row row = iterator.next();
                            if (userid == null) {
                                userid = row.getLong(1);
                            }
                            String searchKeyword = row.getString(5);

                            //这个BUG调了很久
//                            Long clickCategoryId = row.getLong(7);

//                            Long clickCategoryId = row.getAs("click_category_id");
                            Long clickCategoryId = row.getAs(7);

                            // 实际上这里要对数据说明一下
                            // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
                            // 其实，只有搜索行为，是有searchKeyword字段的
                            // 只有点击品类的行为，是有clickCategoryId字段的
                            // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的

                            // 我们决定是否将搜索词或点击品类id拼接到字符串中去
                            // 首先要满足：不能是null值
                            // 其次，之前的字符串中还没有搜索词或者点击品类id

                            if (StringUtils.isNotEmpty(searchKeyword)) {
                                if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                                    searchKeywordsBuffer.append(searchKeyword + ",");
                                }
                            }
                            if (clickCategoryId != null) {
                                if (!clickCategoryIdsBuffer.toString().contains(
                                        String.valueOf(clickCategoryId))) {
                                    clickCategoryIdsBuffer.append(clickCategoryId + ",");
                                }
                            }

                            // 计算session开始和结束时间
                            Date actionTime = DateUtils.parseTime(row.getString(4));

                            if (startTime == null) {
                                startTime = actionTime;
                            }
                            if (endTime == null) {
                                endTime = actionTime;
                            }

                            if (actionTime.before(startTime)) {
                                startTime = actionTime;
                            }
                            if (actionTime.after(endTime)) {
                                endTime = actionTime;
                            }

                            // 计算session访问步长
                            stepLength++;
                        }

                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        // 计算session访问时长（秒）
                        long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                        // 大家思考一下
                        // 我们返回的数据格式，即使<sessionid,partAggrInfo>
                        // 但是，这一步聚合完了以后，其实，我们是还需要将每一行数据，跟对应的用户信息进行聚合
                        // 问题就来了，如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
                        // 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
                        // 如果我们这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
                        // 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举

                        // 所以，我们这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
                        // 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
                        // 然后再直接将返回的Tuple的key设置成sessionid
                        // 最后的数据格式，还是<sessionid,fullAggrInfo>

                        // 聚合数据，用什么样的格式进行拼接？
                        // 我们这里统一定义，使用key=value|key=value
                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                                + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                                + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                                + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                                + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);

                        return new Tuple2<Long, String>(userid, partAggrInfo);
                    }

                });


        System.out.println("userid2PartAggrInfoRDD============" + userid2PartAggrInfoRDD.count());
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
     *
     * @param sc
     * @return
     */
    private static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }


    /**
     * 过滤session数据
     *
     * @param sessionid2AggrInfoRDD
     * @param taskParam
     * @return
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(JavaPairRDD<String, String> sessionid2AggrInfoRDD,
                                                                        final JSONObject taskParam,
                                                                        final Accumulator<String> sessionAggrStatAccumulator) {

        // 为了使用后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串

        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");


        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }
        final String parameter = _parameter;

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(
                new Function<Tuple2<String, String>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        //首先从tuple中获取聚合数据
                        String aggrInfo = tuple._2;


                        //接着，依次按照筛选条件进行过滤
                        //按照年龄范围进行过滤（startAge,endAge）
                        int age = Integer.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_AGE));


                        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
                        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);


                        // 接着，依次按照筛选条件进行过滤
                        // 按照年龄范围进行过滤（startAge、endAge）
                        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                                parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }

                        // 按照职业范围进行过滤（professionals）
                        // 互联网,IT,软件
                        // 互联网
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                                parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        // 按照城市范围进行过滤（cities）
                        // 北京,上海,广州,深圳
                        // 成都
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                                parameter, Constants.PARAM_CITIES)) {
                            return false;
                        }

                        // 按照性别进行过滤
                        // 男/女
                        // 男，女
                        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                                parameter, Constants.PARAM_SEX)) {
                            return false;
                        }

                        // 按照搜索词进行过滤
                        // session可能搜索了 火锅,蛋糕,烧烤
                        // 我们的筛选条件可能是 火锅,串串香,iphone手机
                        // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                        // 任何一个搜索词相当，即通过
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                                parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        // 按照点击品类id进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
                                parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }


                        // 如果经过了之前的多个过滤条件之后，程序能够走到这里
                        // 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
                        // 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
                        // 进行相应的累加计数

                        // 主要走到这一步，那么就是需要计数的session
                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                        // 计算出session的访问时长和访问步长的范围，并进行相应的累加
                        long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                        long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                        calculateVisitLength(visitLength);
                        calculateStepLength(stepLength);

                        return true;
                    }

                    /**
                     * 计算访问时长范围
                     * @param visitLength
                     */
                    private void calculateVisitLength(long visitLength) {
                        if (visitLength >= 1 && visitLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        } else if (visitLength >= 4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        } else if (visitLength >= 7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if (visitLength >= 10 && visitLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        } else if (visitLength > 30 && visitLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if (visitLength > 60 && visitLength <= 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if (visitLength > 180 && visitLength <= 600) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        } else if (visitLength > 600 && visitLength <= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if (visitLength > 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    /**
                     * 计算访问步长范围
                     * @param stepLength
                     */
                    private void calculateStepLength(long stepLength) {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if (stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if (stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if (stepLength >= 10 && stepLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if (stepLength > 30 && stepLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if (stepLength > 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }


                });
        return filteredSessionid2AggrInfoRDD;
    }


    /**
     * 获取通过筛选条件的session的访问明细数据RDD
     *
     * @param sessionid2aggrInfoRDD
     * @param sessionid2actionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionid2detailRDD(
            JavaPairRDD<String, String> sessionid2aggrInfoRDD,
            JavaPairRDD<String, Row> sessionid2actionRDD) {


        JavaPairRDD<String, Row> sessionid2detailRDD = sessionid2aggrInfoRDD
                .join(sessionid2actionRDD)
                .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Row> call(
                            Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                        return new Tuple2<String, Row>(tuple._1, tuple._2._2);
                    }

                });
        return sessionid2detailRDD;
    }


    private static void randomExtractSession(
            JavaSparkContext sc,
            final long taskid,
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionid2actionRDD) {

        /**
         * 第一步，计算出每天每小时的session数量
         */

        // 获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
        JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD.mapToPair(

                new PairFunction<Tuple2<String, String>, String, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(
                            Tuple2<String, String> tuple) throws Exception {

                        String sessionid = tuple._1;
                        String aggrInfo = tuple._2;

                        String startTime = StringUtils.getFieldFromConcatString(
                                aggrInfo, "\\|", Constants.FIELD_START_TIME);
                        String dateHour = DateUtils.getDateHour(startTime);

                        return new Tuple2<String, String>(dateHour, aggrInfo);
                    }

                });


        // 得到每天每小时的session数量
        Map<String, Object> countMap = time2sessionidRDD.countByKey();


        /**
         * 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引
         */
        // 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
        Map<String, Map<String, Long>> dateHourCountMap = new HashMap<String, Map<String, Long>>();

        for (Map.Entry<String, Object> countEntry : countMap.entrySet()) {
            String dateHour = countEntry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];

            long count = Long.valueOf(String.valueOf(countEntry.getValue()));

            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if (hourCountMap == null) {
                hourCountMap = new HashMap<String, Long>();
                dateHourCountMap.put(date, hourCountMap);
            }

            hourCountMap.put(hour, count);
        }

        // 开始实现我们的按时间比例随机抽取算法

        // 总共要抽取100个session，先按照天数，进行平分
        int extractNumberPerDay = 100 / dateHourCountMap.size();
        // <date,<hour,(3,5,20,102)>>

        /**
         * session随机抽取功能
         *
         * 用到了一个比较大的变量，随机抽取索引map
         * 之前是直接在算子里面使用了这个map，每个task都会拷贝一份map副本
         * 还是比较消耗内存和网络传输性能
         * 将map做成广播变量
         *
         */
        Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<String, Map<String, List<Integer>>>();
        Random random = new Random();

        for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

            // 计算出这一天的session总数
            long sessionCount = 0L;
            for (long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }

            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);

            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<String, List<Integer>>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            // 遍历每个小时
            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();

                // 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
                // 就可以计算出，当前小时需要抽取的session数量
                int hourExtractNumber = (int) (((double) count / (double) sessionCount)
                        * extractNumberPerDay);
                if (hourExtractNumber > count) {
                    hourExtractNumber = (int) count;
                }

                // 先获取当前小时的存放随机数的list
                List<Integer> extractIndexList = hourExtractMap.get(hour);

                if (extractIndexList == null) {
                    extractIndexList = new ArrayList<Integer>();
                    hourExtractMap.put(hour, extractIndexList);
                }

                // 生成上面计算出来的数量的随机数
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) count);
                    while (extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) count);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }

        /**
         * fastutil的使用，很简单，比如List<Integer>的list，对应到fastutil，就是IntList
         */
        Map<String, Map<String, IntList>> fastutilDateHourExtractMap =
                new HashMap<String, Map<String, IntList>>();



        for(Map.Entry<String, Map<String, List<Integer>>> dateHourExtractEntry :
                dateHourExtractMap.entrySet()) {
            String date = dateHourExtractEntry.getKey();
            Map<String, List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();

            Map<String, IntList> fastutilHourExtractMap = new HashMap<String, IntList>();

            for(Map.Entry<String, List<Integer>> hourExtractEntry : hourExtractMap.entrySet()) {
                String hour = hourExtractEntry.getKey();
                List<Integer> extractList = hourExtractEntry.getValue();

                IntList fastutilExtractList = new IntArrayList();

                for(int i = 0; i < extractList.size(); i++) {
                    fastutilExtractList.add(extractList.get(i));
                }

                fastutilHourExtractMap.put(hour, fastutilExtractList);
            }

            fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
        }


        final Broadcast<Map<String, Map<String,IntList>>> dateHourExtractMapBroadcast = sc.broadcast(fastutilDateHourExtractMap);


        /**
         * 第三步：遍历每天每小时的session，然后根据随机索引进行抽取
         */

        // 执行groupByKey算子，得到<dateHour,(session aggrInfo)>
        JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();

        // 我们用flatMap算子，遍历所有的<dateHour,(session aggrInfo)>格式的数据
        // 然后呢，会遍历每天每小时的session
        // 如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上
        // 那么抽取该session，直接写入MySQL的random_extract_session表
        // 将抽取出来的session id返回回来，形成一个新的JavaRDD<String>
        // 然后最后一步，是用抽取出来的sessionid，去join它们的访问行为明细数据，写入session表
        JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD.flatMapToPair(

                new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterable<Tuple2<String, String>> call(
                            Tuple2<String, Iterable<String>> tuple)
                            throws Exception {
                        List<Tuple2<String, String>> extractSessionids =
                                new ArrayList<Tuple2<String, String>>();

                        String dateHour = tuple._1;
                        String date = dateHour.split("_")[0];
                        String hour = dateHour.split("_")[1];
                        Iterator<String> iterator = tuple._2.iterator();

                        /**
                         * 使用广播变量的时候
                         * 直接调用广播变量（Broadcast类型）的value() / getValue()
                         * 可以获取到之前封装的广播变量
                         */
                        Map<String, Map<String, IntList>> dateHourExtractMap =
                                dateHourExtractMapBroadcast.value();
                        List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);

                        ISessionRandomExtractDAO sessionRandomExtractDAO =
                                DAOFactory.getSessionRandomExtractDAO();

                        int index = 0;
                        while (iterator.hasNext()) {
                            String sessionAggrInfo = iterator.next();


                            if (extractIndexList.contains(index)) {
                                String sessionid = StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                                // 将数据写入MySQL
                                SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                                sessionRandomExtract.setTaskid(taskid);
                                sessionRandomExtract.setSessionid(sessionid);
                                sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
                                sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                                sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));

                                sessionRandomExtractDAO.insert(sessionRandomExtract);

                                // 将sessionid加入list
                                extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));
                            }

                            index++;
                        }

                        return extractSessionids;
                    }

                });

        /**
         * 第四步：获取抽取出来的session的明细数据
         */
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = extractSessionidsRDD.join(sessionid2actionRDD);

//        extractSessionDetailRDD.foreach(
//                new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
//                    private static final long serialVersionUID = 1L;
//
//                    @Override
//                    public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
//                        Row row = tuple._2._2;
////
//                        SessionDetail sessionDetail = new SessionDetail();
//                        sessionDetail.setTaskid(taskid);
//                        sessionDetail.setUserid(row.getLong(1));
//                        sessionDetail.setSessionid(row.getString(2));
//                        sessionDetail.setPageid(row.getLong(3));
//                        sessionDetail.setActionTime(row.getString(4));
//                        sessionDetail.setSearchKeyword(row.getString(5));
//                        sessionDetail.setClickCategoryId(row.getAs(6) == null ? -1 : row.getLong(6));
//                        sessionDetail.setClickProductId(row.getAs(7) == null ? -1 : row.getLong(7));
//                        sessionDetail.setOrderCategoryIds(row.getString(8));
//                        sessionDetail.setOrderProductIds(row.getString(9));
//                        sessionDetail.setPayCategoryIds(row.getString(10));
//                        sessionDetail.setPayProductIds(row.getString(11));
//
//                        ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
//                        sessionDetailDAO.insert(sessionDetail);
//                    }
//
//

        extractSessionDetailRDD.foreachPartition(

                new VoidFunction<Iterator<Tuple2<String,Tuple2<String,Row>>>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(
                            Iterator<Tuple2<String, Tuple2<String, Row>>> iterator)
                            throws Exception {
                        List<SessionDetail> sessionDetails = new ArrayList<SessionDetail>();

                        while(iterator.hasNext()) {
                            Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();

                            Row row = tuple._2._2;

                            SessionDetail sessionDetail = new SessionDetail();
                            sessionDetail.setTaskid(taskid);
                            sessionDetail.setUserid(row.getLong(1));
                            sessionDetail.setSessionid(row.getString(2));
                            sessionDetail.setPageid(row.getLong(3));
                            sessionDetail.setActionTime(row.getString(4));
                            sessionDetail.setSearchKeyword(row.getString(5));
                            sessionDetail.setClickCategoryId(row.getAs(6) == null ? -1 : row.getLong(6));
                            sessionDetail.setClickProductId(row.getAs(7) == null ? -1 : row.getLong(7));
                            sessionDetail.setOrderCategoryIds(row.getString(8));
                            sessionDetail.setOrderProductIds(row.getString(9));
                            sessionDetail.setPayCategoryIds(row.getString(10));
                            sessionDetail.setPayProductIds(row.getString(11));

                            sessionDetails.add(sessionDetail);
                        }

                        ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                        sessionDetailDAO.insertBatch(sessionDetails);
                    }

                });

    }


    /**
     * 计算各session范围占比，并写入MySQL
     *
     * @param value
     */
    private static void calculateAndPersistAggrStat(String value, long taskid) {
        // 从Accumulator统计串中获取值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double) visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double) step_length_60 / (double) session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }


    /**
     * 获取top10热门品类
     *
     * @param taskid
     * @param sessionid2detailRDD
     * @return
     */
    private static List<Tuple2<CategorySortKey, String>> getTop10Category(Long taskid,
                                                                          JavaPairRDD<String, Row> sessionid2detailRDD) {
        /**
         * 第一步：获取符合条件的session访问过的所有品类
         */

        //获取符合条件的session访问明细

//        JavaPairRDD<String, Row> sessionid2detailRDD = filteredSessionid2AggrInfoRDD.join(sessionid2actionRDD)
//                .mapToPair(
//                        new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
//                            private static final long serialVersionUID = 1L;
//
//                            @Override
//                            public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
//                                return new Tuple2<String, Row>(tuple._1, tuple._2._2);
//                            }
//                        }
//                );

        //获取session访问过的所有品类id
        // 访问过：指的是，点击过、下单过、支付过的品类
        JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;


                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

                        Long clickCategoryId = row.getAs(6) == null ? -1 : row.getLong(6);
                        if (clickCategoryId != -1) {
                            list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
                        }

                        String orderCategoryIds = row.getString(8);
                        if (orderCategoryIds != null) {
                            String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                            for (String orderCategoryId : orderCategoryIdsSplited) {
                                list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId),
                                        Long.valueOf(orderCategoryId)));
                            }
                        }

                        String payCategoryIds = row.getString(10);
                        if (payCategoryIds != null) {
                            String[] payCategoryIdsSplited = payCategoryIds.split(",");
                            for (String payCategoryId : payCategoryIdsSplited) {
                                list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId),
                                        Long.valueOf(payCategoryId)));
                            }
                        }

                        return list;
                    }
                }
        );

        /**
         * 必须要进行去重
         * 如果不去重的话，会出现重复的categoryid，排序会对重复的categoryid已经countInfo进行排序
         * 最后很可能会拿到重复的数据
         */
        categoryidRDD = categoryidRDD.distinct();

        /**
         * 第二步：计算各品类的点击、下单和支付的次数
         */

        // 访问明细中，其中三种访问行为是：点击、下单和支付
        // 分别来计算各品类点击、下单和支付的次数，可以先对访问明细数据进行过滤
        // 分别过滤出点击、下单和支付行为，然后通过map、reduceByKey等算子来进行计算


        // 计算各个品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD =
                getClickCategoryId2CountRDD(sessionid2detailRDD);
        // 计算各个品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD =
                getOrderCategoryId2CountRDD(sessionid2detailRDD);
        // 计算各个品类的支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRDD =
                getPayCategoryId2CountRDD(sessionid2detailRDD);


        /**
         * 第三步：join各品类与它的点击、下单和支付的次数
         *
         * categoryidRDD中，是包含了所有的符合条件的session，访问过的品类id
         *
         * 上面分别计算出来的三份，各品类的点击、下单和支付的次数，可能不是包含所有品类的
         * 比如，有的品类，就只是被点击过，但是没有人下单和支付
         *
         * 所以，这里，就不能使用join操作，要使用leftOuterJoin操作，就是说，如果categoryidRDD不能
         * join到自己的某个数据，比如点击、或下单、或支付次数，那么该categoryidRDD还是要保留下来的
         * 只不过，没有join到的那个数据，就是0了
         *
         */
        JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(
                categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD,
                payCategoryId2CountRDD);


        /**
         * 第四步：自定义二次排序key
         */


        /**
         * 第五步：将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序（降序）
         */
        JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryid2countRDD.mapToPair(

                new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<CategorySortKey, String> call(
                            Tuple2<Long, String> tuple) throws Exception {
                        String countInfo = tuple._2;
                        long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
                        long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
                        long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_PAY_COUNT));

                        CategorySortKey sortKey = new CategorySortKey(clickCount,
                                orderCount, payCount);

                        return new Tuple2<CategorySortKey, String>(sortKey, countInfo);
                    }

                });

        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false);

        /**
         * 第六步：用take(10)取出top10热门品类，并写入MySQL
         */
        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();

        List<Tuple2<CategorySortKey, String>> top10CategoryList =
                sortedCategoryCountRDD.take(10);

        for (Tuple2<CategorySortKey, String> tuple : top10CategoryList) {
            String countInfo = tuple._2;
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_PAY_COUNT));

            Top10Category category = new Top10Category();
            category.setTaskid(taskid);
            category.setCategoryid(categoryid);
            category.setClickCount(clickCount);
            category.setOrderCount(orderCount);
            category.setPayCount(payCount);

            top10CategoryDAO.insert(category);
        }

        return top10CategoryList;

    }


    /**
     * 获取各品类点击次数RDD
     *
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {

        /**
         *
         *
         * 这儿，是对完整的数据进行了filter过滤，过滤出来点击行为的数据
         * 点击行为的数据其实只占总数据的一小部分
         * 所以过滤以后的RDD，每个partition的数据量，很有可能会很不均匀
         * 而且数据量肯定会变少很多
         *
         * 所以针对这种情况，比较合适用一下coalesce算子，在filter过后去减少partition的数量
         *
         */
        JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        Long clickCategoryId = row.getAs(6) == null ? -1 : row.getLong(6);
                        return clickCategoryId != -1 ? true : false;
                    }
                }
        );
//                .coalesce(100);

        /**
         * 对coalesce操作做一个说明
         *
         * 我在这里用的模式都是local模式，主要是用来测试，所以local模式下，不用去设置分区和并行度的数量
         * local模式自己本身就是进程内模拟的集群来执行，本身性能就很高
         * 而且对并行度、partition数量都有一定的内部的优化
         *
         * 这里再自己去设置，就有点画蛇添足
         *
         *
         */

        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(
                new PairFunction<Tuple2<String, Row>, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {

                        long clickCategoryId = tuple._2.getLong(6);
                        return new Tuple2<Long, Long>(clickCategoryId, 1L);
                    }
                }
        );


        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(

                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        return clickCategoryId2CountRDD;
    }


    /**
     * 获取各品类的下单次数RDD
     *
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {

        JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return row.getString(8) != null ? true : false;
                    }
                }
        );
        JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

                    private static final long serialVersionUID = 1L;


                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {

                        Row row = tuple._2;

                        String orderCategoryIds = row.getString(8);

                        String[] orderCategoryIdsSplited = orderCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

                        for (String orderCategoryId : orderCategoryIdsSplited) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));

                        }

                        return list;
                    }
                }
        );


        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(

                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        return orderCategoryId2CountRDD;

    }


    /**
     * 获取各个品类的支付次数RDD
     *
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {


        JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return row.getString(10) != null ? true : false;
                    }
                }
        );
        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

                    private static final long serialVersionUID = 1L;


                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {

                        Row row = tuple._2;

                        String payCategoryIds = row.getString(10);

                        String[] payCategoryIdsSplited = payCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();

                        for (String payCategoryId : payCategoryIdsSplited) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));

                        }

                        return list;
                    }
                }
        );


        JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(

                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        return payCategoryId2CountRDD;

    }


    /**
     * 连接品类RDD与数据RDD
     *
     * @param categoryidRDD
     * @param clickCategoryId2CountRDD
     * @param orderCategoryId2CountRDD
     * @param payCategoryId2CountRDD
     * @return
     */
    private static JavaPairRDD<Long, String> joinCategoryAndData(
            JavaPairRDD<Long, Long> categoryidRDD,
            JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
            JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
            JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
        // 如果用leftOuterJoin，就可能出现，右边那个RDD中，join过来时没有值
        // 所以Tuple中的第二个值用Optional<Long>类型，就代表，可能有值，可能没有值
        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD);

        JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(

                new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(
                            Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple)
                            throws Exception {
                        long categoryid = tuple._1;
                        Optional<Long> optional = tuple._2._2;
                        long clickCount = 0L;

                        if (optional.isPresent()) {
                            clickCount = optional.get();
                        }

                        String value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" +
                                Constants.FIELD_CLICK_COUNT + "=" + clickCount;

                        return new Tuple2<Long, String>(categoryid, value);
                    }

                });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(

                new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(
                            Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
                            throws Exception {
                        long categoryid = tuple._1;
                        String value = tuple._2._1;

                        Optional<Long> optional = tuple._2._2;
                        long orderCount = 0L;

                        if (optional.isPresent()) {
                            orderCount = optional.get();
                        }

                        value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;

                        return new Tuple2<Long, String>(categoryid, value);
                    }

                });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(

                new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(
                            Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
                            throws Exception {
                        long categoryid = tuple._1;
                        String value = tuple._2._1;

                        Optional<Long> optional = tuple._2._2;
                        long payCount = 0L;

                        if (optional.isPresent()) {
                            payCount = optional.get();
                        }

                        value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;

                        return new Tuple2<Long, String>(categoryid, value);
                    }

                });

        return tmpMapRDD;
    }

    /**
     * 获取top10活跃session
     *
     * @param taskid
     * @param sessionid2detailRDD
     */
    private static void getTop10Session(
            JavaSparkContext sc,
            final long taskid,
            List<Tuple2<CategorySortKey, String>> top10CategoryList,
            JavaPairRDD<String, Row> sessionid2detailRDD) {


        /**
         * 第一步：将top10热门品类的id，生成一份RDD
         */
        List<Tuple2<Long, Long>> top10CategoryIdList =
                new ArrayList<Tuple2<Long, Long>>();

        for (Tuple2<CategorySortKey, String> category : top10CategoryList) {
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    category._2, "\\|", Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<Long, Long>(categoryid, categoryid));
        }

        JavaPairRDD<Long, Long> top10CategoryIdRDD =
                sc.parallelizePairs(top10CategoryIdList);


        /**
         * 第二步：计算top10品类被各session点击的次数
         */
        JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD =
                sessionid2detailRDD.groupByKey();

        JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2detailsRDD.flatMapToPair(

                new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterable<Tuple2<Long, String>> call(
                            Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        String sessionid = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();

                        // 计算出该session，对每个品类的点击次数
                        while (iterator.hasNext()) {
                            Row row = iterator.next();

                            if (row.get(6) != null) {
                                long categoryid = row.getLong(6);

                                Long count = categoryCountMap.get(categoryid);
                                if (count == null) {
                                    count = 0L;
                                }

                                count++;

                                categoryCountMap.put(categoryid, count);
                            }
                        }

                        // 返回结果，<categoryid,sessionid,count>格式
                        List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();

                        for (Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()) {
                            long categoryid = categoryCountEntry.getKey();
                            long count = categoryCountEntry.getValue();
                            String value = sessionid + "," + count;
                            list.add(new Tuple2<Long, String>(categoryid, value));
                        }

                        return list;
                    }

                });

        // 获取到to10热门品类，被各个session点击的次数
        JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD
                .join(categoryid2sessionCountRDD)
                .mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(
                            Tuple2<Long, Tuple2<Long, String>> tuple)
                            throws Exception {
                        return new Tuple2<Long, String>(tuple._1, tuple._2._2);
                    }

                });

        /**
         * 第三步：分组取TopN算法实现，获取每个品类的top10活跃用户
         */
        JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD =
                top10CategorySessionCountRDD.groupByKey();

        JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(

                new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterable<Tuple2<String, String>> call(
                            Tuple2<Long, Iterable<String>> tuple)
                            throws Exception {
                        long categoryid = tuple._1;
                        Iterator<String> iterator = tuple._2.iterator();

                        // 定义取topn的排序数组
                        String[] top10Sessions = new String[10];

                        while (iterator.hasNext()) {
                            String sessionCount = iterator.next();
                            long count = Long.valueOf(sessionCount.split(",")[1]);

                            // 遍历排序数组
                            for (int i = 0; i < top10Sessions.length; i++) {
                                // 如果当前i位，没有数据，那么直接将i位数据赋值为当前sessionCount
                                if (top10Sessions[i] == null) {
                                    top10Sessions[i] = sessionCount;
                                    break;
                                } else {
                                    long _count = Long.valueOf(top10Sessions[i].split(",")[1]);

                                    // 如果sessionCount比i位的sessionCount要大
                                    if (count > _count) {
                                        // 从排序数组最后一位开始，到i位，所有数据往后挪一位
                                        for (int j = 9; j > i; j--) {
                                            top10Sessions[j] = top10Sessions[j - 1];
                                        }
                                        // 将i位赋值为sessionCount
                                        top10Sessions[i] = sessionCount;
                                        break;
                                    }

                                    // 比较小，继续外层for循环
                                }
                            }
                        }

                        // 将数据写入MySQL表
                        List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();

                        for (String sessionCount : top10Sessions) {
                            if (sessionCount != null) {
                                String sessionid = sessionCount.split(",")[0];
                                long count = Long.valueOf(sessionCount.split(",")[1]);

                                // 将top10 session插入MySQL表
                                Top10Session top10Session = new Top10Session();
                                top10Session.setTaskid(taskid);
                                top10Session.setCategoryid(categoryid);
                                top10Session.setSessionid(sessionid);
                                top10Session.setClickCount(count);

                                ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
                                top10SessionDAO.insert(top10Session);

                                // 放入list
                                list.add(new Tuple2<String, String>(sessionid, sessionid));
                            }
                        }

                        return list;
                    }

                });


        /**
         * 第四步：获取top10活跃session的明细数据，并写入MySQL
         */
        JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD =
                top10SessionRDD.join(sessionid2detailRDD);
        sessionDetailRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                Row row = tuple._2._2;

                SessionDetail sessionDetail = new SessionDetail();
                sessionDetail.setTaskid(taskid);
                sessionDetail.setUserid(row.getLong(1));
                sessionDetail.setSessionid(row.getString(2));
                sessionDetail.setPageid(row.getAs(3) == null? -1 :row.getLong(3));
                sessionDetail.setActionTime(row.getString(4));
                sessionDetail.setSearchKeyword(row.getString(5));
                sessionDetail.setClickCategoryId(row.getAs(6) == null ? -1 : row.getLong(6));
                sessionDetail.setClickProductId(row.getAs(7) == null ? -1 : row.getLong(7));
                sessionDetail.setOrderCategoryIds(row.getString(8));
                sessionDetail.setOrderProductIds(row.getString(9));
                sessionDetail.setPayCategoryIds(row.getString(10));
                sessionDetail.setPayProductIds(row.getString(11));

                ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                sessionDetailDAO.insert(sessionDetail);
            }
        });


    }
}
