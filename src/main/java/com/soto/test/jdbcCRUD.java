package com.soto.test;


import java.sql.*;

public class jdbcCRUD {


    public static void main(String[] args) {
//		insert();
//		update();
//		delete();
		select();
        preparedStatement();
    }

    private static void insert() {
        Connection conn = null;

        Statement stmt = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");

            conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/spark_project",
                    "root",
                    "123456");

            stmt = conn.createStatement();

            String sql = "insert into test_user(name,age) values('李四',26)";
            int rtn = stmt.executeUpdate(sql);

            System.out.println("SQL语句影响了【" + rtn + "】行。");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 释放数据库连接
            try {
                if(stmt != null) {
                    stmt.close();
                }
                if(conn != null) {
                    conn.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }

    /**
     * 测试更新数据
     */
    private static void update() {
        Connection conn = null;
        Statement stmt = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");

            conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/spark_project",
                    "root",
                    "123456");
            stmt = conn.createStatement();

            String sql = "update test_user set age=27 where name='李四'";
            int rtn = stmt.executeUpdate(sql);

            System.out.println("SQL语句影响了【" + rtn + "】行。");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(stmt != null) {
                    stmt.close();
                }
                if(conn != null) {
                    conn.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }

    /**
     * 测试删除数据
     */
    private static void delete() {
        Connection conn = null;
        Statement stmt = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");

            conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/spark_project",
                    "root",
                    "123456");
            stmt = conn.createStatement();

            String sql = "delete from test_user where name='李四'";
            int rtn = stmt.executeUpdate(sql);

            System.out.println("SQL语句影响了【" + rtn + "】行。");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(stmt != null) {
                    stmt.close();
                }
                if(conn != null) {
                    conn.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
    /**
     * 测试查询数据
     */
    private static void select() {
        Connection conn = null;
        Statement stmt = null;
        // 对于select查询语句，需要定义ResultSet
        // ResultSet就代表了，你的select语句查询出来的数据
        // 需要通过ResutSet对象，来遍历你查询出来的每一行数据，然后对数据进行保存或者处理
        ResultSet rs = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");

            conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/spark_project",
                    "root",
                    "123456");
            stmt = conn.createStatement();

            String sql = "select * from test_user";
            rs = stmt.executeQuery(sql);

            // 获取到ResultSet以后，就需要对其进行遍历，然后获取查询出来的每一条数据
            while(rs.next()) {
                int id = rs.getInt(1);
                String name = rs.getString(2);
                int age = rs.getInt(3);
                System.out.println("id=" + id + ", name=" + name + ", age=" + age);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(stmt != null) {
                    stmt.close();
                }
                if(conn != null) {
                    conn.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }

    private static void preparedStatement() {
        Connection conn = null;

        PreparedStatement pstmt = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");

            conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/spark_project?characterEncoding=utf8",
                    "root",
                    "123456");

            // 第一个，SQL语句中，值所在的地方，都用问好代表
            String sql = "insert into test_user(name,age) values(?,?)";

            pstmt = conn.prepareStatement(sql);

            // 第二个，必须调用PreparedStatement的setX()系列方法，对指定的占位符设置实际的值
            pstmt.setString(1, "李四");
            pstmt.setInt(2, 26);

            // 第三个，执行SQL语句时，直接使用executeUpdate()即可，不用传入任何参数
            int rtn = pstmt.executeUpdate();

            System.out.println("SQL语句影响了【" + rtn + "】行。");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(pstmt != null) {
                    pstmt.close();
                }
                if(conn != null) {
                    conn.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }


}
