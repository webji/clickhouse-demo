package org.example.clickhousedemo.common;

import org.example.clickhousedemo.cluster.query.JobPriority;
import org.example.clickhousedemo.cluster.query.QueryJobStatus;

public interface Job {
    void setStatus(QueryJobStatus status);
    String getName();
    JobPriority getPriority();
    void setResult(String result);
}
