package org.example.clickhousedemo.cluster.db;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.example.clickhousedemo.common.JobQueue;
import org.example.clickhousedemo.cluster.query.QueryJob;
import org.example.clickhousedemo.cluster.query.QueryJobStatus;
import org.example.clickhousedemo.common.SQLCache;
import org.example.clickhousedemo.cluster.config.Config;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Data
@Builder
public class TableClusterManager {

    String tableName;
    /**
     * ip - HostConnectionManager
     */
    Map<String, HostConnectionManager> hostConnectionManagerMap;

    // keeps track of current sql -> connection
//    Map<String, HostConnectionKeeper> sqlConnectionMap;
    /**
     * sql - QueryJob
     */
    ConcurrentHashMap<String, List<QueryJob>> sqlQueryJobMap;

    JobQueue resultQueue;
    SQLCache sqlCache;
    Config config;

    public void addHostConnectionManager(HostConnectionManager hostConnectionManager) {
        if (hostConnectionManagerMap == null) {
            hostConnectionManagerMap = new HashMap<>();
            hostConnectionManagerMap.put(hostConnectionManager.getIp(), hostConnectionManager);
        }
        HostConnectionManager manager = hostConnectionManagerMap.get(hostConnectionManager.getIp());
        if (manager == null) {
            hostConnectionManagerMap.put(hostConnectionManager.getIp(), hostConnectionManager);
        }
    }

    public void query(QueryJob job) {
        log.debug("TableClusterMaager Received: [job=" + job + "]");
        job.setTableClusterManager(this);
        doSqlQueryJob(job);
    }

    public void doSqlQueryJob(QueryJob queryJob) {
        String sql = queryJob.getSql();
        List<QueryJob> jobList = sqlQueryJobMap.get(sql);
        if (jobList == null) {
            log.debug("No Same sql querying, add to sqlQueryJobMap, [sql=" + sql + "]");
            jobList = new ArrayList<>();
            jobList.add(queryJob);
            sqlQueryJobMap.put(sql, jobList);
            HostConnectionManager hostConnectionManager = chooseHost(sql);
            log.debug("Chosed the HostConnectionManager: [ip=" + hostConnectionManager.getIp() + "]");
            hostConnectionManager.query(queryJob);
            complete(queryJob);
        } else {
            jobList.add(queryJob);
            log.debug("The Same sql already in SqlQueryJobMap: [sql=" + sql + "][count=" + jobList.size() + "]");
        }
    }

    public boolean cachedAndAdd(QueryJob job) {
        String sql = job.getSql();
        List<QueryJob> jobList = sqlQueryJobMap.get(sql);
        if (jobList == null) {
            return false;
        }
        log.debug("Duplicate Sql Querying, Just add to List and Wait");
        jobList.add(job);
        return true;
    }

    public boolean isFree() {
        for (HostConnectionManager hostConnectionManager : hostConnectionManagerMap.values()) {
            if(hostConnectionManager.isFree()) {
                return true;
            }
        }
        return false;
    }

    public HostConnectionManager chooseHost(String sql) {
        Integer lastConnection = Integer.MAX_VALUE;
        HostConnectionManager lastHostConnectionManager = null;
        for (HostConnectionManager hostConnectionManager : hostConnectionManagerMap.values()) {
            Integer connections = hostConnectionManager.currentConnections();
            if (connections < lastConnection) {
                lastConnection = connections;
                lastHostConnectionManager = hostConnectionManager;
            }
        }
        return lastHostConnectionManager;
    }

    public void complete(QueryJob job) {
        job.setTableClusterManager(null);
        String sql = job.getSql();
        if (config.getCacheEnabled()) {
            String result = job.getResult();
            sqlCache.put(sql, result);
            log.debug("Update Cache: [sql=" + sql + "][result=" + result + "]");
        }
        List<QueryJob> jobList = sqlQueryJobMap.get(sql);
        if (jobList == null || jobList.size() <= 0) {
            log.error("Failed to get JobList for sql, [sql=" + sql + "]");
            return;
        }
        jobList.stream().forEach(j -> {
            j.setStatus(QueryJobStatus.SENDING);
            j.setResult(job.getResult());
            if ((j == job && !j.getSync()) || j!=job) {
                log.debug("Adding Job to resultQueue, [sql=" + j.getSql() + "]");
                resultQueue.push(j, j.getPriority());
                log.debug("Added Job to resultQueue, [sql=" + j.getSql() + "]");
            }
        });
        sqlQueryJobMap.remove(sql);
    }
}
