package com.soto.test;

/**
 * 单例模式
 */
public class Singleton {
    // 首先必须有一个私有的静态变量，来引用自己即将被创建出来的单例
    private static Singleton instance = null;

    /**
     * 其次，必须对自己的构造方法使用private进行私有化
     * 保证外界的代码不能随意的创建类的实例
     */
    private Singleton() {

    }

    /**
     *
     * @return
     */
    public static Singleton getInstance() {
        // 两步检查机制
        // 首先第一步，多个线程过来的时候，判断instance是否为null
        // 如果为null再往下走
        if(instance == null) {
            // 在这里，进行多个线程的同步
            // 同一时间，只能有一个线程获取到Singleton Class对象的锁
            synchronized(Singleton.class) {
                // 只有第一个获取到锁的线程，进入到这里，会发现是instance是null
                // 然后才会去创建这个单例
                if(instance == null) {
                    instance = new Singleton();
                }
            }
        }
        return instance;
    }
}
