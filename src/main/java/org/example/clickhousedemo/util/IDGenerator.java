package org.example.clickhousedemo.util;

public class IDGenerator {
//    private static IDGenerator instance;
    private static Integer nextId = 0;

//    private IDGenerator() {
//
//    }
//
//    public static synchronized IDGenerator getInstance() {
//        if (instance == null) {
//            instance = new IDGenerator();
//        }
//        return instance;
//    }

    public static synchronized Integer pickId() {
        Integer ret = new Integer(nextId);
        nextId ++;
        return ret;
    }
}
