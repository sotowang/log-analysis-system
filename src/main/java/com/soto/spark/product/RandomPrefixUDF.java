package com.soto.spark.product;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

public class RandomPrefixUDF implements UDF2<String, Integer, String> {

    private static final long serialVersionUID = 1L;
    @Override
    public String call(String val, Integer integer) throws Exception {
        Random random = new Random();
        int randNum = random.nextInt(10);
        return randNum + "_" + val;
    }
}
