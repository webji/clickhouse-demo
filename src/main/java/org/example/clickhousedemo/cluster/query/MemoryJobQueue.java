package org.example.clickhousedemo.cluster.query;

import com.fasterxml.jackson.core.PrettyPrinter;
import lombok.extern.slf4j.Slf4j;
import org.example.clickhousedemo.common.Job;
import org.example.clickhousedemo.common.JobQueue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
public class MemoryJobQueue implements JobQueue {
    Map<JobPriority, ConcurrentLinkedQueue> jobQueue;
    Object lock;

    public MemoryJobQueue() {
        jobQueue = new HashMap<>();
        jobQueue.put(JobPriority.HIGH, new ConcurrentLinkedQueue<Job>());
        jobQueue.put(JobPriority.MEDIUM, new ConcurrentLinkedQueue<Job>());
        jobQueue.put(JobPriority.LOW, new ConcurrentLinkedQueue<Job>());
        lock = new Object();
    }

    @Override
    public void push(Job job, JobPriority priority) {
        job.setStatus(QueryJobStatus.QUEUED);
        ConcurrentLinkedQueue queue = jobQueue.get(priority);
        queue.offer(job);
        lockNotify();
    }

    @Override
    public Job pop() {
        for (JobPriority priority : JobPriority.values()) {
            ConcurrentLinkedQueue<Job> queue = jobQueue.get(priority);
            Job job = queue.poll();
            if (job != null) {
                return job;
            }
        }
        return null;
    }

    @Override
    public Map<JobPriority, Integer> count() {
        Map<JobPriority, Integer> countMap = new HashMap<>();
        for (JobPriority priority : JobPriority.values()) {
            ConcurrentLinkedQueue jobs = jobQueue.get(priority);
            countMap.put(priority, jobs.size());
        }
        return countMap;
    }

    @Override
    public Map<JobPriority, List<Job>> jobMap() {
        Map<JobPriority, List<Job>> jobMap = new HashMap<>();
        for (JobPriority priority : JobPriority.values()) {
            ConcurrentLinkedQueue jobs = jobQueue.get(priority);
            List<Job> jobList = new ArrayList<>();
            jobList.addAll(jobs);
            jobMap.put(priority, jobList);
        }
        return jobMap;
    }

    @Override
    public boolean isEmpty() {
        for (JobPriority priority : JobPriority.values()) {
            ConcurrentLinkedQueue jobs = jobQueue.get(priority);
            if (!jobs.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int size() {
        int size = 0;
        for (JobPriority priority : JobPriority.values()) {
            ConcurrentLinkedQueue jobs = jobQueue.get(priority);
            size += jobs.size();
        }
        return size;
    }

    public void lockWait() {
        synchronized (lock) {
            try {
                lock.wait();
            } catch (InterruptedException e) {
                log.error("Exception: ", e);
            }
        }
    }

    public void lockNotify() {
        synchronized (lock) {
            try {
                lock.notify();
            } catch (Exception e) {
                log.error("Exception: ", e);
            }
        }
    }

}
