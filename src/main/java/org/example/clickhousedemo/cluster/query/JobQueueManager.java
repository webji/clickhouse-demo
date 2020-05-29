package org.example.clickhousedemo.cluster.query;

import org.example.clickhousedemo.common.JobQueue;
import org.springframework.stereotype.Component;

@Component
public class JobQueueManager {
    JobQueue jobQueue = new MemoryJobQueue();
}
