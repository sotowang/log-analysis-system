package com.soto.spark.ad;

import com.google.common.base.Optional;
import com.soto.conf.ConfigurationManager;
import com.soto.constant.Constants;
import com.soto.dao.*;
import com.soto.dao.factory.DAOFactory;
import com.soto.domain.*;
import com.soto.util.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 *  广告点击流量实时统计spark作业
 */
public class AdClickRealTimeSpark {

    public static void main(String[] args) {
        // 构建Spark Streaming上下文
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("AdClickRealTimeStatSpark")
                ;
//                .set("spark.streaming.receiver.writeAheadLog.enable", "true"); ;   //预写日志


        // spark streaming的上下文是构建JavaStreamingContext对象
        // 而不是像之前的JavaSparkContext、SQLContext/HiveContext
        // 传入的第一个参数，和之前的spark上下文一样，也是SparkConf对象；第二个参数则不太一样

        // 第二个参数是spark streaming类型作业比较有特色的一个参数
        // 实时处理batch的interval
        // spark streaming，每隔一小段时间，会去收集一次数据源（kafka）中的数据，做成一个batch
        // 每次都是处理一个batch中的数据

        // 通常来说，batch interval，就是指每隔多少时间收集一次数据源中的数据，然后进行处理
        // 一遍spark streaming的应用，都是设置数秒到数十秒（很少会超过1分钟）

        // 项目中，设置5秒钟的batch interval
        // 每隔5秒钟，spark streaming作业就会收集最近5秒内的数据源接收过来的数据
        JavaStreamingContext jssc = new JavaStreamingContext(
                conf, Durations.seconds(5));


        jssc.checkpoint("hdfs://sotowang-pc:9000/streaming_checkpoint");

        // 创建针对Kafka数据来源的输入DStream（离线流，代表了一个源源不断的数据来源，抽象）
        // 构建kafka参数map
        // 主要要放置的就是，你要连接的kafka集群的地址（broker集群的地址列表）
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list",
                ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));


        // 构建topic set
        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(",");

        Set<String> topics = new HashSet<String>();
        for (String kafkaTopic : kafkaTopicsSplited) {
            topics.add(kafkaTopic);
        }

        // 基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream
        // 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
        JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);


        // 根据动态黑名单进行数据过滤
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = filterByBlacklist(adRealTimeLogDStream);

        // 生成动态黑名单
        generateDynamicBlacklist(filteredAdRealTimeLogDStream).print();


        // 业务功能一：计算广告点击流量实时统计结果（yyyyMMdd_province_city_adid,clickCount）
        JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(
                filteredAdRealTimeLogDStream);


        // 业务功能二：实时统计每天每个省份top3热门广告
        // 统计的稍微细一些了
        calculateProvinceTop3Ad(adRealTimeStatDStream);


        // 业务功能三：实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势（每分钟的点击量）
        // 统计的非常细了
        // 我们每次都可以看到每个广告，最近一小时内，每分钟的点击量
        // 每支广告的点击趋势
        calculateAdClickCountByWindow(adRealTimeLogDStream);




        // 构建完spark streaming上下文之后，要进行上下文的启动、等待执行结束、关闭
        jssc.start();
        jssc.awaitTermination();
        jssc.close();


    }




    private static JavaPairDStream<String, String> filterByBlacklist(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair(

                new Function<JavaPairRDD<String,String>, JavaPairRDD<String,String>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public JavaPairRDD<String, String> call(
                            JavaPairRDD<String, String> rdd) throws Exception {

                        // 首先，从mysql中查询所有黑名单用户，将其转换为一个rdd
                        IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
                        List<AdBlacklist> adBlacklists = adBlacklistDAO.findAll();

                        List<Tuple2<Long, Boolean>> tuples = new ArrayList<Tuple2<Long, Boolean>>();

                        for(AdBlacklist adBlacklist : adBlacklists) {
                            tuples.add(new Tuple2<Long, Boolean>(adBlacklist.getUserid(), true));
                        }

                        JavaSparkContext sc = new JavaSparkContext(rdd.context());
                        JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuples);

                        // 将原始数据rdd映射成<userid, tuple2<string, string>>
                        JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(
                                new PairFunction<Tuple2<String,String>, Long, Tuple2<String, String>>() {

                            private static final long serialVersionUID = 1L;

                            @Override
                            public Tuple2<Long, Tuple2<String, String>> call(
                                    Tuple2<String, String> tuple)
                                    throws Exception {
                                String log = tuple._2;
                                String[] logSplited = log.split(" ");
                                long userid = Long.valueOf(logSplited[3]);
                                return new Tuple2<Long, Tuple2<String, String>>(userid, tuple);
                            }

                        });

                        // 将原始日志数据rdd，与黑名单rdd，进行左外连接
                        // 如果说原始日志的userid，没有在对应的黑名单中，join不到，左外连接
                        // 用inner join，内连接，会导致数据丢失

                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD =
                                mappedRDD.leftOuterJoin(blacklistRDD);

                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD.filter(

                                new Function<Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>>, Boolean>() {

                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Boolean call(
                                            Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple)
                                            throws Exception {
                                        Optional<Boolean> optional = tuple._2._2;

                                        // 如果这个值存在，那么说明原始日志中的userid，join到了某个黑名单用户
                                        if(optional.isPresent() && optional.get()) {
                                            return false;
                                        }

                                        return true;
                                    }

                                });

                        JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(

                                new PairFunction<Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>>, String, String>() {

                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Tuple2<String, String> call(
                                            Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple)
                                            throws Exception {
                                        return tuple._2._1;
                                    }

                                });

                        return resultRDD;
                    }

                });

        return filteredAdRealTimeLogDStream;
    }


    /**
     * 生成动态黑名单
     * @param filteredAdRealTimeLogDStream
     * @return
     */
    private static JavaPairDStream<String, Long> generateDynamicBlacklist(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {

        // 将日志的格式处理成<yyyyMMdd_userid_adid, 1L>格式
        JavaPairDStream<String, Long> yyyyMMdd_userid_adidRDD = filteredAdRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                        String logs = tuple._2;
                        String[] splited = logs.split(" ");
                        long userid = Long.valueOf(splited[3]);
                        String timestamp = splited[0];
                        Date date = new Date(Long.valueOf(timestamp));
                        String dateKey = DateUtils.formatDateKey(date);
                        String adid = splited[4];

                        String key = dateKey + "_" + userid + "_" + adid;

                        return new Tuple2<String, Long>(key, 1L);
                    }
                }
        );

        // 针对处理后的日志格式，执行reduceByKey算子即可
        // （每个batch中）每天每个用户对每个广告的点击量
        JavaPairDStream<String,Long> day_user_ad_countRDD = yyyyMMdd_userid_adidRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        day_user_ad_countRDD.foreachRDD(
                new Function<JavaPairRDD<String, Long>, Void>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
                        rdd.foreachPartition(
                                new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                                        List<AdUserClickCount> adUserClickCountList = new ArrayList<AdUserClickCount>();

                                        while (iterator.hasNext()) {
                                            Tuple2<String, Long> tuple = iterator.next();
                                            String log = tuple._1;
                                            long count = tuple._2;
                                            String[] split = log.split("_");
                                            String date_ = split[0];
                                            String date = date_.substring(0, 4) + "-" + date_.substring(4, 6) + "-" + date_.substring(6, 8);
                                            long userid = Long.valueOf(split[1]);
                                            long adid = Long.valueOf(split[2]);

                                            AdUserClickCount adUserClickCount = new AdUserClickCount();
                                            adUserClickCount.setAdid(adid);
                                            adUserClickCount.setClickCount(count);
                                            adUserClickCount.setUserid(userid);
                                            adUserClickCount.setDate(date);

                                            adUserClickCountList.add(adUserClickCount);

                                        }

                                        IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                                        if (!adUserClickCountList.isEmpty()) {
                                            adUserClickCountDAO.updateBatch(adUserClickCountList);
                                        }

                                    }
                                }
                        );
                        return null;
                    }
                }
        );

        JavaPairDStream<String, Long> blackListDStream = day_user_ad_countRDD.filter(
                new Function<Tuple2<String, Long>, Boolean>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<String, Long> tuple) throws Exception {
                        String log = tuple._1;
                        String[] split = log.split("_");

                        String date_ = split[0];
                        String date = date_.substring(0, 4) + "-" + date_.substring(4, 6) + "-" + date_.substring(6, 8);
                        long userid = Long.valueOf(split[1]);
                        long adid = Long.valueOf(split[2]);

                        IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                        int count = adUserClickCountDAO.findClickCountByMultiKey(date, userid, adid);

                        if (count > 1) {
                            return true;
                        }else
                            return false;

                    }
                }
        );

        //对userid进行去重
        JavaDStream<Long> blackListUseridDStream = blackListDStream.map(
                new Function<Tuple2<String, Long>, Long>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Tuple2<String, Long> tuple) throws Exception {
                        String log = tuple._1;
                        Long userid = Long.valueOf(log.split("_")[1]);
                        return userid;
                    }

                }
        );

        JavaDStream<Long> distinctUserid = blackListUseridDStream.transform(
                new Function<JavaRDD<Long>, JavaRDD<Long>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JavaRDD<Long> call(JavaRDD<Long> rdd) throws Exception {
                        return rdd.distinct();
                    }
                }
        );

        //将黑名单写入mysql
        distinctUserid.foreachRDD(
                new Function<JavaRDD<Long>, Void>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Void call(JavaRDD<Long> rdd) throws Exception {

                        rdd.foreachPartition(
                                new VoidFunction<Iterator<Long>>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void call(Iterator<Long> iterator) throws Exception {
                                        List<AdBlacklist> adBlacklistList = new ArrayList<AdBlacklist>();

                                        while (iterator.hasNext()) {
                                            long userid = iterator.next();
                                            AdBlacklist adBlacklist = new AdBlacklist();

                                            adBlacklist.setUserid(userid);
                                            adBlacklistList.add(adBlacklist);
                                        }

                                        IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
                                        if (!adBlacklistList.isEmpty()) {
                                            adBlacklistDAO.insertBatch(adBlacklistList);
                                        }
                                    }
                                }
                        );
                        return null;
                    }
                }

        );

        return blackListDStream;
    }



    /**
     * 计算广告点击流量实时统计
     * @param filteredAdRealTimeLogDStream
     * @return
     */
    private static JavaPairDStream<String, Long> calculateRealTimeStat(
            JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        // 业务逻辑一
        // 广告点击流量实时统计


        // date province city userid adid
        // date_province_city_adid，作为key；1作为value
        // 通过spark，直接统计出来全局的点击次数，在spark集群中保留一份；在mysql中，也保留一份
        // 对原始数据进行map，映射成<date_province_city_adid,1>格式
        // 然后对上述格式的数据，执行updateStateByKey算子
        // spark streaming特有的一种算子，在spark集群内存中，维护一份key的全局状态
        JavaPairDStream<String, Long> mappedDStream = filteredAdRealTimeLogDStream.mapToPair(

                new PairFunction<Tuple2<String,String>, String, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple)
                            throws Exception {
                        String log = tuple._2;
                        String[] logSplited = log.split(" ");

                        String timestamp = logSplited[0];
                        Date date = new Date(Long.valueOf(timestamp));
                        String datekey = DateUtils.formatDateKey(date);	// yyyyMMdd

                        String province = logSplited[1];
                        String city = logSplited[2];
                        long adid = Long.valueOf(logSplited[4]);

                        String key = datekey + "_" + province + "_" + city + "_" + adid;

                        return new Tuple2<String, Long>(key, 1L);
                    }

                });

        // 在这个dstream中，就相当于，有每个batch rdd累加的各个key（各天各省份各城市各广告的点击次数）
        // 每次计算出最新的值，就在aggregatedDStream中的每个batch rdd中反应出来
        JavaPairDStream<String, Long> aggregatedDStream = mappedDStream.updateStateByKey(

                new Function2<List<Long>, Optional<Long>, Optional<Long>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Optional<Long> call(List<Long> values, Optional<Long> optional)
                            throws Exception {
                        // 举例来说
                        // 对于每个key，都会调用一次这个方法
                        // 比如key是<20151201_Jiangsu_Nanjing_10001,1>，就会来调用一次这个方法7
                        // 10个

                        // values，(1,1,1,1,1,1,1,1,1,1)

                        // 首先根据optional判断，之前这个key，是否有对应的状态
                        long clickCount = 0L;

                        // 如果说，之前是存在这个状态的，那么就以之前的状态作为起点，进行值的累加
                        if(optional.isPresent()) {
                            clickCount = optional.get();
                        }

                        // values，代表了，batch rdd中，每个key对应的所有的值
                        for(Long value : values) {
                            clickCount += value;
                        }

                        return Optional.of(clickCount);
                    }

                });

        // 将计算出来的最新结果，同步一份到mysql中，以便于j2ee系统使用
        aggregatedDStream.foreachRDD(new Function<JavaPairRDD<String,Long>, Void>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {

                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator)
                            throws Exception {
                        List<AdStat> adStats = new ArrayList<AdStat>();

                        while(iterator.hasNext()) {
                            Tuple2<String, Long> tuple = iterator.next();

                            String[] keySplited = tuple._1.split("_");
                            String date_ = keySplited[0];
                            String date = date_.substring(0, 4) + "-" + date_.substring(4, 6) + "-" + date_.substring(6, 8);
                            String province = keySplited[1];
                            String city = keySplited[2];
                            long adid = Long.valueOf(keySplited[3]);

                            long clickCount = tuple._2;

                            AdStat adStat = new AdStat();
                            adStat.setDate(date);
                            adStat.setProvince(province);
                            adStat.setCity(city);
                            adStat.setAdid(adid);
                            adStat.setClickCount(clickCount);

                            adStats.add(adStat);
                        }

                        IAdStatDAO adStatDAO = DAOFactory.getAdStatDAO();
                        if (!adStats.isEmpty()) {

                            adStatDAO.updateBatch(adStats);
                        }
                    }

                });

                return null;
            }

        });

        return aggregatedDStream;
    }


    /**
     * 计算每天各省份的top3热门广告
     * @param adRealTimeStatDStream
     */
    private static void calculateProvinceTop3Ad(
            JavaPairDStream<String, Long> adRealTimeStatDStream) {
        // adRealTimeStatDStream
        // 每一个batch rdd，都代表了最新的全量的每天各省份各城市各广告的点击量

        JavaDStream<Row> rowsDStream =  adRealTimeStatDStream.transform(
                new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {

                        // <yyyyMMdd_province_city_adid, clickCount>
                        // <yyyyMMdd_province_adid, clickCount>

                        // 计算出每天各省份各广告的点击量
                        JavaPairRDD<String, Long> mappedRDD = rdd.mapToPair(

                                new PairFunction<Tuple2<String,Long>, String, Long>() {

                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Tuple2<String, Long> call(
                                            Tuple2<String, Long> tuple) throws Exception {
                                        String[] keySplited = tuple._1.split("_");
                                        String date = keySplited[0];
                                        String province = keySplited[1];
                                        long adid = Long.valueOf(keySplited[3]);
                                        long clickCount = tuple._2;

                                        String key = date + "_" + province + "_" + adid;

                                        return new Tuple2<String, Long>(key, clickCount);
                                    }

                                });

                        JavaPairRDD<String, Long> dailyAdClickCountByProvinceRDD = mappedRDD.reduceByKey(

                                new Function2<Long, Long, Long>() {

                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Long call(Long v1, Long v2)
                                            throws Exception {
                                        return v1 + v2;
                                    }

                                });

                        // 将dailyAdClickCountByProvinceRDD转换为DataFrame
                        // 注册为一张临时表
                        // 使用Spark SQL，通过开窗函数，获取到各省份的top3热门广告

                        JavaRDD<Row> rowsRDD = dailyAdClickCountByProvinceRDD.map(

                                new Function<Tuple2<String,Long>, Row>() {

                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Row call(Tuple2<String, Long> tuple)
                                            throws Exception {
                                        String[] keySplited = tuple._1.split("_");
                                        String date_ = keySplited[0];
                                        String province = keySplited[1];
                                        long adid = Long.valueOf(keySplited[2]);
                                        long clickCount = tuple._2;

                                        String date = date_.substring(0, 4) + "-" + date_.substring(4, 6) + "-" + date_.substring(6, 8);

//                                        String date = DateUtils.formatDate(DateUtils.parseDateKey(datekey));

                                        return RowFactory.create(date, province, adid, clickCount);
                                    }

                                });

                        StructType schema = DataTypes.createStructType(Arrays.asList(
                                DataTypes.createStructField("date", DataTypes.StringType, true),
                                DataTypes.createStructField("province", DataTypes.StringType, true),
                                DataTypes.createStructField("ad_id", DataTypes.LongType, true),
                                DataTypes.createStructField("click_count", DataTypes.LongType, true)));

                        HiveContext sqlContext = new HiveContext(rdd.context());

                        DataFrame dailyAdClickCountByProvinceDF = sqlContext.createDataFrame(rowsRDD, schema);

                        // 将dailyAdClickCountByProvinceDF，注册成一张临时表
                        dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_click_count_by_prov");

                        // 使用Spark SQL执行SQL语句，配合开窗函数，统计出各身份top3热门的广告
                        DataFrame provinceTop3AdDF = sqlContext.sql(
                                "SELECT "
                                        + "date,"
                                        + "province,"
                                        + "ad_id,"
                                        + "click_count "
                                        + "FROM ( "
                                        + "SELECT "
                                        + "date,"
                                        + "province,"
                                        + "ad_id,"
                                        + "click_count,"
                                        + "ROW_NUMBER() OVER(PARTITION BY province ORDER BY click_count DESC) rank "
                                        + "FROM tmp_daily_ad_click_count_by_prov "
                                        + ") t "
                                        + "WHERE rank>=3"
                        );

                        return provinceTop3AdDF.javaRDD();
                    }
                }
        );

        // rowsDStream
        // 每次都是刷新出来各个省份最热门的top3广告
        // 将其中的数据批量更新到MySQL中
        rowsDStream.foreachRDD(new Function<JavaRDD<Row>, Void>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Void call(JavaRDD<Row> rdd) throws Exception {

                rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Iterator<Row> iterator) throws Exception {
                        List<AdProvinceTop3> adProvinceTop3s = new ArrayList<AdProvinceTop3>();

                        while(iterator.hasNext()) {
                            Row row = iterator.next();
                            String date = row.getString(0);
                            String province = row.getString(1);
                            long adid = row.getLong(2);
                            long clickCount = row.getLong(3);

                            AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
                            adProvinceTop3.setDate(date);
                            adProvinceTop3.setProvince(province);
                            adProvinceTop3.setAdid(adid);
                            adProvinceTop3.setClickCount(clickCount);

                            adProvinceTop3s.add(adProvinceTop3);
                        }

                        IAdProvinceTop3DAO adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO();
                        if (!adProvinceTop3s.isEmpty()) {
                            adProvinceTop3DAO.updateBatch(adProvinceTop3s);
                        }
                    }

                });

                return null;
            }

        });

    }

    /**
     * 计算最近1小时滑动窗口内的广告点击趋势
     *
     * @param adRealTimeLogDStream
     */
    private static void calculateAdClickCountByWindow(JavaPairInputDStream<String, String> adRealTimeLogDStream) {

        // 映射成<yyyyMMddHHMM_adid,1L>格式
        JavaPairDStream<String, Long> pairDStream = adRealTimeLogDStream.mapToPair(

                new PairFunction<Tuple2<String,String>, String, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple)
                            throws Exception {
                        // timestamp province city userid adid
                        String[] logSplited = tuple._2.split(" ");
                        String timeMinute = DateUtils.formatTimeMinute(
                                new Date(Long.valueOf(logSplited[0])));
                        long adid = Long.valueOf(logSplited[4]);

                        return new Tuple2<String, Long>(timeMinute + "_" + adid, 1L);
                    }

                });

        // 过来的每个batch rdd，都会被映射成<yyyyMMddHHMM_adid,1L>的格式
        // 每次出来一个新的batch，都要获取最近1小时内的所有的batch
        // 然后根据key进行reduceByKey操作，统计出来最近一小时内的各分钟各广告的点击次数
        // 1小时滑动窗口内的广告点击趋势
        // 点图 / 折线图

        JavaPairDStream<String, Long> aggrRDD = pairDStream.reduceByKeyAndWindow(

                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }

                }, Durations.minutes(60), Durations.seconds(10));


        // aggrRDD
        // 每次都可以拿到，最近1小时内，各分钟（yyyyMMddHHMM）各广告的点击量
        // 各广告，在最近1小时内，各分钟的点击量
        aggrRDD.foreachRDD(new Function<JavaPairRDD<String,Long>, Void>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Void call(JavaPairRDD<String, Long> rdd) throws Exception {

                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator)
                            throws Exception {
                        List<AdClickTrend> adClickTrends = new ArrayList<AdClickTrend>();

                        while(iterator.hasNext()) {
                            Tuple2<String, Long> tuple = iterator.next();
                            String[] keySplited = tuple._1.split("_");
                            // yyyyMMddHHmm
                            String dateMinute = keySplited[0];
                            long adid = Long.valueOf(keySplited[1]);
                            long clickCount = tuple._2;

                            String date_ = dateMinute.substring(0, 8);
                            String date = date_.substring(0, 4) + "-" + date_.substring(4, 6) + "-" + date_.substring(6, 8);

                            String hour = dateMinute.substring(8, 10);
                            String minute = dateMinute.substring(10);

                            AdClickTrend adClickTrend = new AdClickTrend();
                            adClickTrend.setDate(date);
                            adClickTrend.setHour(hour);
                            adClickTrend.setMinute(minute);
                            adClickTrend.setAdid(adid);
                            adClickTrend.setClickCount(clickCount);

                            adClickTrends.add(adClickTrend);
                        }

                        IAdClickTrendDAO adClickTrendDAO = DAOFactory.getAdClickTrendDAO();

                        if (!adClickTrends.isEmpty()) {

                            adClickTrendDAO.updateBatch(adClickTrends);
                        }
                    }

                });

                return null;
            }

        });
    }

    private static void testDriverHA() {
        final String checkpointDir = "hdfs://sotowang-pc/streaming_checkpoint";

        JavaStreamingContextFactory contextFactory = new JavaStreamingContextFactory() {

            @Override
            public JavaStreamingContext create() {
                SparkConf conf = new SparkConf()
                        .setMaster("local[2]")
                        .setAppName("AdClickRealTimeStatSpark");

                JavaStreamingContext jssc = new JavaStreamingContext(
                        conf, Durations.seconds(5));
                jssc.checkpoint(checkpointDir);

                Map<String, String> kafkaParams = new HashMap<String, String>();
                kafkaParams.put(Constants.KAFKA_METADATA_BROKER_LIST,
                        ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
                String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
                String[] kafkaTopicsSplited = kafkaTopics.split(",");
                Set<String> topics = new HashSet<String>();
                for(String kafkaTopic : kafkaTopicsSplited) {
                    topics.add(kafkaTopic);
                }

                JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
                        jssc,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParams,
                        topics);

                JavaPairDStream<String, String> filteredAdRealTimeLogDStream =
                        filterByBlacklist(adRealTimeLogDStream);
                generateDynamicBlacklist(filteredAdRealTimeLogDStream).print();

                JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(
                        filteredAdRealTimeLogDStream);
                calculateProvinceTop3Ad(adRealTimeStatDStream);
                calculateAdClickCountByWindow(adRealTimeLogDStream);

                return jssc;
            }

        };

        JavaStreamingContext context = JavaStreamingContext.getOrCreate(
                checkpointDir, contextFactory);
        context.start();
        context.awaitTermination();
    }
}
