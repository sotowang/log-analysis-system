package com.soto;

import com.google.common.base.Optional;
import com.soto.conf.ConfigurationManager;
import com.soto.constant.Constants;
import com.soto.dao.IAdBlacklistDAO;
import com.soto.dao.IAdUserClickCountDAO;
import com.soto.dao.factory.DAOFactory;
import com.soto.domain.AdBlacklist;
import com.soto.domain.AdUserClickCount;
import com.soto.util.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

public class sparkstreaming_kafkaTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("sparkstreaming_kafkaTest")
                .setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(20));

        Map<String,String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));

        String kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplit = kafkaTopics.split(",");

        Set<String> topics = new HashSet<>();

        for (String topic : kafkaTopicsSplit) {
            topics.add(topic);
        }

        JavaPairInputDStream<String,String> adRealTimeLogDStream = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        adRealTimeLogDStream.print();


        JavaPairDStream<String,String> filteredAdRealTimeLogDStream = filterByBlacklist(adRealTimeLogDStream);
//        filteredAdRealTimeLogDStream.print();

        // 生成动态黑名单
        generateDynamicBlacklist(filteredAdRealTimeLogDStream).print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }



    private static JavaPairDStream<String,String> filterByBlacklist(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair(
                new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {

                        //在Mysql中获取黑名单
                        IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
                        List<AdBlacklist> adBlacklists = adBlacklistDAO.findAll();

                        List<Tuple2<Long, Boolean>> tuples = new ArrayList<Tuple2<Long, Boolean>>();
                        for (AdBlacklist adBlacklist : adBlacklists) {
                            tuples.add(new Tuple2<Long, Boolean>(adBlacklist.getUserid(), true));
                        }

                        //将tuples 转为RDD
                        JavaSparkContext sc = new JavaSparkContext(rdd.context());
                        JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuples);


                        // 将原始数据rdd映射成<userid, tuple2<string, string>>
                        JavaPairRDD<Long, Tuple2<String, String>> userid2tupleRDD = rdd.mapToPair(
                                new PairFunction<Tuple2<String, String>, Long, Tuple2<String, String>>() {
                                    private static final long serialVersionUID = 1L;
                                    @Override
                                    public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, String> tuple) throws Exception {
                                        String logs = tuple._2;
                                        String[] splited = logs.split(" ");



                                        long userid = Long.valueOf(splited[3]);

                                        return new Tuple2<Long, Tuple2<String, String>>(userid, tuple);
                                    }
                                }
                        );

                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD = userid2tupleRDD.leftOuterJoin(blacklistRDD);
                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD.filter(
                                new Function<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, Boolean>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Boolean call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple) throws Exception {
                                        Optional<Boolean> optional = tuple._2._2;

                                        // 如果这个值存在，那么说明原始日志中的userid，join到了某个黑名单用户
                                        if (optional.isPresent()) {
                                            return false;
                                        }
                                        return true;
                                    }
                                }
                        );

                        JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(
                                new PairFunction<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, String, String>() {
                                    @Override
                                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple) throws Exception {
                                        return tuple._2._1;
                                    }

                                }
                        );


//                        System.out.println(adBlacklists.get(0).getUserid());

                        return resultRDD;
                    }
                }
        );
        return filteredAdRealTimeLogDStream;
    }

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
                                        adUserClickCountDAO.updateBatch(adUserClickCountList);

                                    }
                                }
                        );
                        return null;
                    }
                }
        );

        return day_user_ad_countRDD;
    }
}
