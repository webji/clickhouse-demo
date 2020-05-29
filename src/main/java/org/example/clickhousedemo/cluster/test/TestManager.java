package org.example.clickhousedemo.cluster.test;


import lombok.Data;
import org.example.clickhousedemo.common.TestJob;
import org.example.clickhousedemo.http.ResponseMessage;
import org.example.clickhousedemo.http.request.TestRequestBody;

import java.util.HashMap;
import java.util.Map;

@Data
public class TestManager {
    Map<String, TestJob> testTaskMap;

    private static TestManager instance;

    private TestManager() {
        testTaskMap = new HashMap<>();
    }

    public static synchronized TestManager getInstance() {
        if (instance == null) {
            instance = new TestManager();
        }
        return instance;
    }









}
