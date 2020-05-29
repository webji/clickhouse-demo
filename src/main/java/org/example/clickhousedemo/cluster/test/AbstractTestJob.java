package org.example.clickhousedemo.cluster.test;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.example.clickhousedemo.common.JobWorker;
import org.example.clickhousedemo.common.TestJob;
import org.example.clickhousedemo.http.request.QueryRequestBody;
import org.example.clickhousedemo.http.request.TestRequestBody;
import org.example.clickhousedemo.util.JsonUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Data
public abstract class AbstractTestJob implements Serializable, TestJob {
    String name;

    @JsonIgnore
    TestRequestBody testConfig;
    @JsonIgnore
    ExecutorService threadExectors;
    @JsonIgnore
    List<QueryRequestBody> queryList;
    @JsonIgnore
    List<JobWorker> workers;


    public AbstractTestJob(TestRequestBody testConfig) {
        this.testConfig = testConfig;
        queryList = JsonUtil.objectOfResourceFile(testConfig.getFileName(), List.class);
        threadExectors = Executors.newFixedThreadPool(testConfig.getThreadNum());
        workers = new ArrayList<>();
    }

    public JobWorker getWorker(int i) {
        return null;
    }

    @Override
    public void start() {
        for (int i = 0; i < testConfig.getThreadNum(); i++) {
            JobWorker jobWorker = getWorker(i);
            workers.add(jobWorker);
            threadExectors.submit(jobWorker);
        }
    }

    @Override
    public void pause() {
        for (JobWorker jobWorker : workers) {
            jobWorker.pause();
        }
    }

    @Override
    public void resume() {
        for (JobWorker jobWorker : workers) {
            jobWorker.resume();
        }
    }

    @Override
    public void stop() {
        for (JobWorker jobWorker : workers) {
            jobWorker.stop();
        }
    }
}
