package com.soto.conf;

import java.io.InputStream;
import java.util.Properties;

public class ConfigurationManager {
    private static Properties prop = new Properties();

    static {
        try {
            // 通过一个“类名.class”的方式，就可以获取到这个类在JVM中对应的Class对象
            // 然后再通过这个Class对象的getClassLoader()方法，就可以获取到当初加载这个类的JVM
            // 中的类加载器（ClassLoader），然后调用ClassLoader的getResourceAsStream()这个方法
            // 就可以用类加载器，去加载类加载路径中的指定的文件
            // 最终可以获取到一个，针对指定文件的输入流（InputStream）
            InputStream in = ConfigurationManager.class
                    .getClassLoader().getResourceAsStream("my.properties");

            // 调用Properties的load()方法，给它传入一个文件的InputStream输入流
            // 即可将文件中的符合“key=value”格式的配置项，都加载到Properties对象中
            // 加载过后，此时，Properties对象中就有了配置文件中所有的key-value对了
            // 然后外界其实就可以通过Properties对象获取指定key对应的value
            prop.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取指定key对应的value
     *
     * 第一次外界代码，调用ConfigurationManager类的getProperty静态方法时，JVM内部会发现
     * ConfigurationManager类还不在JVM的内存中
     *
     * 此时JVM，就会使用自己的ClassLoader（类加载器），去对应的类所在的磁盘文件（.class文件）中
     * 去加载ConfigurationManager类，到JVM内存中来，并根据类内部的信息，去创建一个Class对象
     * Class对象中，就包含了类的元信息，包括类有哪些field（Properties prop）；有哪些方法（getProperty）
     *
     * 加载ConfigurationManager类的时候，还会初始化这个类，那么此时就执行类的static静态代码块
     * 此时静态代码块中的代码，就会加载my.properites文件的内容，到Properties对象中来
     *
     * 下一次外界代码，再调用ConfigurationManager的getProperty()方法时，就不会再次加载类，不会再次初始化
     * 类，和执行静态代码块
     *
     * @param key
     * @return value
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    /**
     * 获取整数类型的配置项
     * @param key
     * @return value
     */
    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取布尔类型的配置项
     * @param key
     * @return value
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 获取Long类型的配置项
     * @param key
     * @return
     */
    public static Long getLong(String key) {
        String value = getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }
}
