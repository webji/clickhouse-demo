package org.example.clickhousedemo.cluster.db;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.example.clickhousedemo.cluster.query.MemoryJobQueue;
import org.example.clickhousedemo.common.Job;
import org.example.clickhousedemo.common.JobQueue;
import org.example.clickhousedemo.cluster.query.QueryJob;
import org.example.clickhousedemo.cluster.query.QueryJobStatus;
import org.example.clickhousedemo.common.JobWorker;
import org.example.clickhousedemo.common.SQLCache;
import org.example.clickhousedemo.cluster.config.Config;
import org.example.clickhousedemo.http.ResponseMessage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Data
@ToString(exclude = {"sqlCache", "config", "resultQueue", "hostConnectionManagerMap", "hostConnectionLock", "sqlQueryJobMap", "jobQueue", "threadExecutor"})
public class TableClusterManager {
    String tableName;
    @JsonIgnore
    SQLCache sqlCache;
    @JsonIgnore
    Config config;
    @JsonIgnore
    JobQueue resultQueue;
    /**
     * ip - HostConnectionManager
     */
    @JsonIgnore
    Map<String, HostConnectionManager> hostConnectionManagerMap;
    @JsonIgnore
    Object hostConnectionLock;

    /**
     * sql - QueryJob
     */
    @JsonIgnore
    ConcurrentHashMap<String, List<QueryJob>> sqlQueryJobMap;

    /**
     * JobQueue
     */
    @JsonIgnore
    JobQueue jobQueue;
    @JsonIgnore
    ExecutorService threadExecutor;

    public TableClusterManager(String tableName) {
        this.tableName = tableName;
        sqlQueryJobMap = new ConcurrentHashMap<>();
        jobQueue = new MemoryJobQueue();
        hostConnectionManagerMap = new HashMap<>();
        hostConnectionLock = new Object();
        threadExecutor = Executors.newSingleThreadExecutor();
        QueryWorker queryWorker = new QueryWorker("QueryWroker-" + tableName);
        threadExecutor.submit(queryWorker);
    }

    public void addHostConnectionManager(HostConnectionManager hostConnectionManager) {
        HostConnectionManager manager = hostConnectionManagerMap.get(hostConnectionManager.getIp());
        if (manager == null) {
            hostConnectionManagerMap.put(hostConnectionManager.getIp(), hostConnectionManager);
        }
    }

    public ResponseMessage query(QueryJob job) {
        log.debug("TableClusterMaager Received: [job=" + job + "]");
        job.setTableClusterManager(this);
        if (loginListAndFirst(job)) {
            return doQuery(job);
        }
        if (job.getSync()) {
            log.debug("Not a first query, and a same sql job is running, just wait here for wakeup");
            return waitJob(job);
        }
        log.debug("Return Job: " + job);
        return ResponseMessage.success(job);
    }

    public ResponseMessage waitJob(QueryJob job) {
        synchronized (job) {
            try {
                log.debug("Try to wait: " + job);
                job.wait();
                log.debug("Succeed to wakeup by job" + job);
            } catch (InterruptedException e) {
                log.error("Exception: ", e);
                return ResponseMessage.fail("Job Wait Interrupted: " + job);
            }
        }
        return ResponseMessage.success(job);
    }

    public ResponseMessage notifyJob(QueryJob job) {
        synchronized (job) {
            try {
                log.debug("Try to notify: " + job);
                job.notify();
                log.debug("Succeed to notify job: " + job);
            } catch (Exception e) {
                log.error("Exception: ", e);
                return ResponseMessage.fail("Job Wait Interrupted: " + job);
            }
        }
        return ResponseMessage.success(job);
    }

    public ResponseMessage doQuery(QueryJob job) {
        HostConnectionManager hostConnectionManager = obtainHostConnectionManager();
        if (hostConnectionManager != null) {
            log.debug("Obtained hostConnectionManager: " + hostConnectionManager);
            hostConnectionManager.query(job);
            if (job.getSync()) {
                // in sync mode, job already got result
                return complete(job);
            } else {
                // in async mode, just return the job
                return ResponseMessage.success(job);
            }
        }
        job.setResult("Failed to obtain HostConnectionManager");
        return ResponseMessage.fail("Failed to obtain HostConnectionManager", job);
    }

    public HostConnectionManager obtainHostConnectionManager() {
        HostConnectionManager availableManager = null;
        synchronized (hostConnectionLock) {
            try {
                while ((availableManager =  chooseAvailableManager()) == null) {
                    hostConnectionLock.wait(10);
                }
            } catch (InterruptedException e) {
                log.info("hostConnectionLock.wait is Interrupted: ", e);
            }
        }
        return availableManager;
    }

    public boolean loginListAndFirst(QueryJob job) {
        String sql = job.getSql();
        List<QueryJob> jobList = sqlQueryJobMap.get(sql);
        if (jobList == null) {
            log.debug("loginList: A first Added Job: " + job);
            jobList = new ArrayList<>();
            jobList.add(job);
            sqlQueryJobMap.put(sql, jobList);
            return true;
        }

        if (log.isDebugEnabled()) {
            StringBuilder idList = new StringBuilder();
            jobList.stream().forEach(j -> {
                idList.append(j.getId() + ",");
            });
            log.debug("Duplicate Sql Querying, Just add to List and Wait, " + idList.toString());
        }

        jobList.add(job);
        return false;
    }

    public HostConnectionManager chooseAvailableManager() {
        Integer lastConnection = Integer.MIN_VALUE;
        HostConnectionManager lastHostConnectionManager = null;
        for (HostConnectionManager hostConnectionManager : hostConnectionManagerMap.values()) {
            Integer availableConnections = hostConnectionManager.availableConnections();
            if (availableConnections > lastConnection) {
                lastConnection = availableConnections;
                lastHostConnectionManager = hostConnectionManager;
            }
        }
        return lastHostConnectionManager;
    }

    public void updateCache(QueryJob job) {
        if (config.getCacheEnabled()) {
            String sql = job.getSql();
            String result = job.getResult();
            if (!result.equalsIgnoreCase("ERROR")) {
                sqlCache.put(sql, result);
                log.debug("Update Cache: [sql=" + sql + "][result=" + result + "]");
            } else {
                log.debug("Error happed, skip add to cache");
            }
        }
    }

    public void logoutList(QueryJob job) {
        String sql = job.getSql();
        List<QueryJob> jobList = sqlQueryJobMap.get(sql);
        if (jobList == null || jobList.size() <= 0) {
            log.error("Failed to get JobList for sql, [sql=" + sql + "]");
            return;
        }
        log.debug("Try to wakeup all logList jobs");
        jobList.stream().forEach(j -> {
            j.setStatus(QueryJobStatus.SENDING);
            j.setResult(job.getResult());
            if (!j.getSync()) {
                log.debug("Try Add Job to resultQueue, [waiting async job=" + j + "][current job=" + job + "]");
                resultQueue.push(j, j.getPriority());
                log.debug("Succeed Add Job to resultQueue, [waiting async job=" + j + "][current job=" + job + "]");
            } else {
                if (j != job) {
                    log.debug("Try to nofity sync job [waiting job=" + j + "][current job=" + job + "]");
                    notifyJob(j);
                } else {
                    log.debug("Self is a sync job, no need to wakeup others, job=" + j);
                }
            }
        });
        sqlQueryJobMap.remove(sql);
        log.debug("Remove sql and job from logList, [sql=" + sql + "][job=" + job + "]");
    }

    public ResponseMessage complete(QueryJob job) {
        log.debug("Enter complete, " + job);
        job.setTableClusterManager(null);
        updateCache(job);
        logoutList(job);
        return ResponseMessage.success(job);
    }

    class QueryWorker extends JobWorker {
        public QueryWorker(String name) {
            super(name);
        }

        @Override
        public void doJob() {
            QueryJob job;
            while ((job = (QueryJob)jobQueue.pop()) != null) {
                log.debug("Pick Job [sql=" + job.getSql() + "]");
                if (loginListAndFirst(job)) {
                    HostConnectionManager hostConnectionManager = obtainHostConnectionManager();
                    if (hostConnectionManager != null) {
                        hostConnectionManager.query(job);
                    } else {
                        jobQueue.push(job, job.getPriority());
                    }
                }
            }
            jobQueue.lockWait();
        }
    }
}
