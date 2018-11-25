package com.soto.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.soto.constant.Constants;
import com.soto.dao.ITaskDAO;
import com.soto.dao.impl.DAOFactory;
import com.soto.domain.Task;
import com.soto.util.ParamUtils;
import com.soto.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Date;

/**
 * 页面单跳转化率模块spark作业
 */
public class PageOneStepConvertRateSpark {
    public static void main(String[] args) {
        //1.构造spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_PAGE)
                ;

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());


        SparkUtils.setMaster(conf);


        //2.生成模拟数据
        SparkUtils.mockData(sc,sqlContext);


        //3.查询任务，获取任务的参数
        long taskid = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PAGE);
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskid);
        if (task == null) {
            System.out.println(new Date() + ": cannot find this task with id [" + taskid + "]");
            return;
        }

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());



        //4.查询指定日期范围内的用户访问行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);




    }
}
