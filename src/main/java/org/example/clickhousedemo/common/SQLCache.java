package org.example.clickhousedemo.common;

import java.util.List;

public interface SQLCache {
    void put(String key, String value);
    String get(String key);
    boolean containsKey(String key);
    int clear();
    int count();
    List<String> keyList();
}
