package com.soto.test;

import com.soto.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JDBC辅助组件测试类
 */
public class JDBCHelperTest {


    public static void main(String[] args) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
//        jdbcHelper.executeUpdate(
//                "insert into test_user(name,age) values(?,?)",
//                new Object[]{"王二", 28});
        //测试查询语句
        final Map<String,Object> testUser = new HashMap<String,Object>();

        jdbcHelper.executeQuery(
                "select name,age from test_user where id=?",
                new Object[]{4},
                new JDBCHelper.QueryCallback(){

                    @Override
                    public void process(ResultSet rs) throws Exception {
                        if (rs.next()) {
                            String name = rs.getString(1);
                            int age = rs.getInt(2);
                            testUser.put("name", name);
                            testUser.put("age", age);
                        }
                    }
                }
        );
        System.out.println(testUser.get("name") + ":" + testUser.get("age"));


        //测试批量
        String sql = "insert into test_user(name,age) values(?,?)";

        List<Object[]> paramsList = new ArrayList<Object[]>();
        paramsList.add(new Object[]{"麻子", 30});
        paramsList.add(new Object[]{"王五", 25});

        jdbcHelper.executeBatch(sql, paramsList);

    }


}
