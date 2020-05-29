package org.example.clickhousedemo.controller;

import org.example.clickhousedemo.cluster.test.TestJobFactory;
import org.example.clickhousedemo.cluster.test.TestManager;
import org.example.clickhousedemo.common.TestJob;
import org.example.clickhousedemo.http.ResponseMessage;
import org.example.clickhousedemo.http.request.TestRequestBody;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/test")
public class TesterController {

    @PostMapping("/add")
    public ResponseMessage addTestJob(@RequestBody TestRequestBody requestBody) {
        Map<String, TestJob> testJobMap = TestManager.getInstance().getTestTaskMap();
        long time = System.currentTimeMillis();
        TestJob testJob = TestJobFactory.testJobOfReqest(requestBody);
        testJob.setName(String.valueOf(time));
        testJobMap.put(testJob.getName(), testJob);
        testJob.start();
        return ResponseMessage.success(testJob);
    }

    @GetMapping("/{jobName}/pause")
    public ResponseMessage pauseTestJob(@PathVariable(name = "jobName") String jobName) {
        Map<String, TestJob> testJobMap = TestManager.getInstance().getTestTaskMap();
        TestJob testJob = testJobMap.get(jobName);
        if (testJob != null) {
            testJob.pause();
            return ResponseMessage.success(testJob);
        } else {
            return ResponseMessage.fail("Failed To Find TestJob: [hashCode=" + jobName + "][testTaskMap=" + testJob + "]" );
        }
    }

    @GetMapping("/{jobName}/resume")
    public ResponseMessage resumeTestJob(@PathVariable(name = "jobName") String jobName) {
        Map<String, TestJob> testJobMap = TestManager.getInstance().getTestTaskMap();
        TestJob testJob = testJobMap.get(jobName);
        if (testJob != null) {
            testJob.resume();
            return ResponseMessage.success(testJob);
        } else {
            return ResponseMessage.fail("Failed To Find TestJob: [hashCode=" + jobName + "][testTaskMap=" + testJobMap + "]" );
        }
    }

    @GetMapping("/{jobName}/stop")
    public ResponseMessage stopTestJob(@PathVariable(name = "jobName") String jobName) {
        Map<String, TestJob> testJobMap = TestManager.getInstance().getTestTaskMap();
        TestJob testJob = testJobMap.get(jobName);
        if (testJob != null) {
            testJob.stop();
            return ResponseMessage.success(testJob);
        } else {
            return ResponseMessage.fail("Failed To Find TestJob: [hashCode=" + jobName + "][testTaskMap=" + testJobMap + "]" );
        }
    }

    @GetMapping("/list")
    public ResponseMessage listJobs() {
        Map<String, TestJob> testJobMap = TestManager.getInstance().getTestTaskMap();
        return ResponseMessage.success(testJobMap);
    }




}
