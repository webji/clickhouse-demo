package org.example.clickhousedemo.common;

import org.example.clickhousedemo.cluster.query.JobPriority;

import java.util.List;
import java.util.Map;

public interface JobQueue extends Queue {
    void push(Job job, JobPriority priority);
    Job pop();
    Map<JobPriority, Integer> count();
    Map<JobPriority, List<Job>> jobMap();

    void lockWait();
    void lockNotify();
}
