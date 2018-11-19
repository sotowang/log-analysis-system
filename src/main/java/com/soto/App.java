package com.soto;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        Long s = null;
        if (-1 != s) {
            System.out.println("bbb");
        }

        if (String.valueOf(s) != String.valueOf(-1)) {
            System.out.println("aaa");
        }
    }
}
