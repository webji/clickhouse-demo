package org.example.clickhousedemo.cluster.test;

import org.example.clickhousedemo.common.TestJob;
import org.example.clickhousedemo.http.request.TestRequestBody;

public class TestJobFactory {
    public static TestJob testJobOfReqest(TestRequestBody testConfig) {
        TestJob testJob = null;
        if (testConfig.getType().equalsIgnoreCase("HTTP")) {
            testJob = new PostTestJob(testConfig);
        } else if (testConfig.getType().equalsIgnoreCase("QUEUE")) {

        }
        return testJob;
    }
}
