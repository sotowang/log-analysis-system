package com.soto.spark.ad;

import com.soto.conf.ConfigurationManager;
import com.soto.constant.Constants;
import com.soto.util.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
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
                .setAppName("AdClickRealTimeStatSpark");


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
        for(String kafkaTopic : kafkaTopicsSplited) {
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


        // 计算出每5秒内的数据中，每天每个用户每个广告的点击量
        // 通过对原始实时日志的处理
        // 将日志的格式处理成<yyyyMMdd_userid_adid, 1L>格式
        JavaPairDStream<String, Long> dailyUserAdClickDStream = adRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {

                        // 从tuple中获取到每一条原始的实时日志
                        String log = tuple._2;
                        String[] logSplited = log.split(" ");

                        // 提取出日期（yyyyMMdd）、userid、adid
                        String timestamp = logSplited[0];
                        Date date = new Date(Long.valueOf(timestamp));
                        String datekey = DateUtils.formatDateKey(date);

                        long userid = Long.valueOf(logSplited[3]);
                        long adid = Long.valueOf(logSplited[4]);

                        // 拼接key
                        String key = datekey + "_" + userid + "_" + adid;

                        return new Tuple2<String, Long>(key, 1L);
                    }
                }
        );

        // 针对处理后的日志格式，执行reduceByKey算子即可
        // （每个batch中）每天每个用户对每个广告的点击量
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(
                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;


                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );








        // 构建完spark streaming上下文之后，记得要进行上下文的启动、等待执行结束、关闭
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }


}
