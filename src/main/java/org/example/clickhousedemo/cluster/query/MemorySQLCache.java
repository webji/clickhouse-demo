package org.example.clickhousedemo.cluster.query;

import lombok.extern.slf4j.Slf4j;
import org.example.clickhousedemo.common.SQLCache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class MemorySQLCache implements SQLCache {
    Map<String, String> cacheMap = new HashMap<>();


    @Override
    public void put(String key, String value) {
        cacheMap.put(key, value);
    }

    @Override
    public String get(String key) {
        return cacheMap.get(key);
    }

    @Override
    public boolean containsKey(String key) {
        return cacheMap.containsKey(key);
    }

    @Override
    public int clear() {
        int count = cacheMap.size();
        cacheMap.clear();
        return count;
    }

    @Override
    public int count() {
        return cacheMap.size();
    }

    @Override
    public List<String> keyList() {
        List<String> ret = new ArrayList<>();
        ret.addAll(cacheMap.keySet());
        return ret;
    }

}
