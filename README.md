### 移动计算大作业
* 针对移动计算中任意一个场景，开发一套系统

```
1. 给出应用场景的详细描述
2. 使用关键技术 “上下文感知”
3. 给出关键技术的设计思想的实现方式
4. 以大作业报告的方式完成，并最终提交整个项目的源代码
```

#### 本项目以spark为基础进行开发
##### 1. 公共组件
```$xslt
* 配置管理组件
* JDBC辅助组件
* 工具类
* 模拟数据生成程序
* 单元测试
* domain，dao
```
    

##### 2. 第一个模块：用户访问session分析模块
* 基础
```$xslt
session粒度聚合，按筛选条件进行过滤
```
* session聚合统计

```$xslt
统计出访问时长和访问步长，各个区间范围的session数量，占总session数量的比例
```

* session随机抽取

```$xslt
按时间比例，随机抽取100个session
```

* top10 热门品类

```$xslt
获取通过筛选投机倒把的session，点击，下单和支付次数最多的10个品类
```

* top10 活跃session

```$xslt
获取top10 热门品类中，每个品类点击次数最多的10个session
```
##### 3. 第二个模块：页面单跳转化率
* 用户的页面访问和页面跳转行为

##### 4. 第三个模块：各区域最热门top3商品

* 用户的商品点击行为

* UDAF函数

* RDD转换DataFrame，注册临时表

* 开窗函数

* Spark SQL数据倾斜解决

---

##### 5. 第四个模块：广告流量实时统计

* 用户的广告点击行为 



##### 6. 技术点和知识点

* 大数据项目的架构（公共组件的封装，包的划分，代码的规范）

* 复杂的分析需求（纯spark作业代码）

* Spark Core 算子的综合应用：map reduce count group

* 算定义Accumulator，按时间比例随机抽取算法，二次排序，分组取TopN算法

* 大数据项目开发流程：数据调研 需求分析 技术方案设计 数据库设计 编码实现 单元测试 本地测试

#####  7. 性能调优

###### 7.1 常规调优

* 性能调优

executor, cpu per executor, memory per executor, driver memory

```$xslt
spark-submit \
--class com.soto.....  \
--num-executors 3 \  
--driver-memory 100m \
--executor-memory 1024m \
--executor-cores 3 \
/usr/local/......jar  \
```

* Kryo 序列化

```$xslt
1. 算子函数中用到了外部变量，会序列化，会使用Kyro
2. 使用了序列化的持久化级别时，在将每个RDD partition序列化成一个在的字节数组时，就会使用Kryo进一步优化序列化的效率和性能
3. stage中task之间 执行shuffle时，文件通过网络传输，会使用序列化
```

###### 7.2 JVM调优


###### 7.3 shuffle调优

###### 7.4 spark算子调优

* 数据倾斜解决
* troubleshotting

##### 8. 生产环境测试
* Hive表测试

```$xslt
hive> create table user_visit_action( 
        date string,    
        user_id bigint, 
        session_id string,  
        page_id bigint, 
        action_time string, 
        search_keyword string,  
        click_category_id bigint,   
        click_product_id bigint,    
        order_category_ids string,  
        order_product_ids string,
        pay_category_ids string,    
        pay_product_ids string,
        city_id bigint  
        );

hive> load data local inpath '/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/sparkhomework/user_visit_action.txt' overwrite into table user_visit_action;

hive> create table user_info(
    user_id bigint,
    username string,
    name string,
    age int,
    professional string,
    city string,
    sex string
    );

hive> load data local inpath '/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/sparkhomework/user_info.txt' into table user_info;


hive> create table product_info(
      product_id bigint,
      product_name string,
      extend_info string
      ); 
      
hive> load data local inpath '/home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/sparkhomework/product_info.txt' into table product_info;


```





