package org.example.clickhousedemo.cluster.query;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.example.clickhousedemo.cluster.config.Config;
import org.example.clickhousedemo.cluster.config.ConfigManager;
import org.example.clickhousedemo.cluster.db.ClickHouseHost;
import org.example.clickhousedemo.cluster.db.DBClusterManager;
import org.example.clickhousedemo.cluster.db.HostConnectionManager;
import org.example.clickhousedemo.cluster.db.TableClusterManager;
import org.example.clickhousedemo.common.Job;
import org.example.clickhousedemo.common.JobQueue;
import org.example.clickhousedemo.common.JobWorker;
import org.example.clickhousedemo.common.SQLCache;
import org.example.clickhousedemo.http.ResponseMessage;
import org.example.clickhousedemo.util.HttpUtil;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Data
//@Component
public class QueryManager {
    private static QueryManager instance;
    /**
     * DBClisuterManager, relatedt to physical architecture
     */
//    @Resource DBClusterManager dbClusterManager;
    /**
     * Pending Job Queue
     */
    JobQueue jobQueue = new MemoryJobQueue();
    /**
     * Result Pending Job Queue
     */
    JobQueue resultQueue = new MemoryJobQueue();
//    ConcurrentLinkedQueue<Job> resultQueue = new ConcurrentLinkedQueue<>();

    /**
     * All Supported Tables
     */
    Set<String> tableNameSet = new HashSet<>();

    /**
     * TableClusterManager binded to Table
     */
    Map<String, TableClusterManager> tableClusterManagerMap = new HashMap<>();

    SQLCache sqlCache = new MemorySQLCache();

    ExecutorService threadExecutor;
    QueryWorker queryWorker;
    Object queryLock;

    ResultWorder resultWorder;
    Object resultLock;

    Config config;

    private QueryManager() {
        initialize();
        threadExecutor = Executors.newFixedThreadPool(2);
        queryWorker = new QueryWorker("QueryWorker");
        resultWorder = new ResultWorder("ResultWorker");
        threadExecutor.submit(queryWorker);
        threadExecutor.submit(resultWorder);
    }

    public static synchronized QueryManager getInstance() {
        if (instance == null) {
            instance = new QueryManager();
        }
        return instance;
    }

    public void initialize() {
        config = ConfigManager.getInstance().getConfig();
        DBClusterManager dbClusterManager = DBClusterManager.getInstance();
         List<ClickHouseHost> clickHouseHosts = dbClusterManager.getDbCluster().getHosts();
        for (ClickHouseHost host : clickHouseHosts) {
            tableNameSet.addAll(host.getTables());
        }
        for (String tableName: tableNameSet) {
            TableClusterManager tableClusterManager = tableClusterManagerMap.get(tableName);
            if (tableClusterManager == null) {
                tableClusterManager = TableClusterManager.builder()
                        .tableName(tableName)
                        .resultQueue(resultQueue)
                        .sqlQueryJobMap(new ConcurrentHashMap<>())
                        .sqlCache(sqlCache)
                        .config(config)
                        .build();
                tableClusterManagerMap.put(tableName, tableClusterManager);
            }
            for (HostConnectionManager hostConnectionManager : dbClusterManager.getHostConnectionManagerMap().values()) {
                if (hostConnectionManager.hasTable(tableName)) {
                    tableClusterManager.addHostConnectionManager(hostConnectionManager);
                }
            }
        }
    }



    public ResponseMessage query(QueryJob job) {
        boolean cacheEnabled = config.getCacheEnabled();
        boolean cacheHit = false;
        String sql = job.getSql();

        // Cache Enabled
        if (cacheEnabled) {
            cacheHit = sqlCache.containsKey(sql);
        }

        if (cacheEnabled && cacheHit) {
            log.debug("Cache Enabled and Hit: [key=" + sql + "]");
            job.setResult(sqlCache.get(sql));
            if (!job.getSync()) {
                resultQueue.push(job, job.getPriority());
            } else {
                job.setStatus(QueryJobStatus.SENDING);
            }
        } else {
            String tableName = job.getTableName();
            if (!tableNameSet.contains(tableName)) {
                return ResponseMessage.fail("Table Not Existed: [tableName=" + tableName + "]", job);
            }
            if (job.getSync()) {
                TableClusterManager tableClusterManager = tableClusterManagerMap.get(tableName);
                tableClusterManager.query(job);
                job.setStatus(QueryJobStatus.SENDING);
            } else {
                log.debug("Push Job to jobQueue: [sql=" + job.getSql() + "]");
                jobQueue.push(job, job.getPriority());
            }
        }
        return ResponseMessage.success(job);
    }


    class QueryWorker extends JobWorker {


        public QueryWorker(String name) {
            super(name);
        }

        @Override
        public void doJob() {
            Job job = jobQueue.pop();
            QueryJob queryJob = (QueryJob) job;
            if (queryJob != null) {
                doSyncJob(queryJob);
            }
        }

        private void doSyncJob(QueryJob queryJob) {
            if (queryJob != null) {
                log.debug("Pick Job [sql=" + queryJob.getSql() + "]");
                String tableName = queryJob.getName();
                TableClusterManager tableClusterManager = tableClusterManagerMap.get(tableName);
                if (tableClusterManager == null) {
                    log.error("Failed to Process job", queryJob);
                    queryJob.setResult("Failed to process Job, tableName is not supported");
                    resultQueue.push(queryJob, queryJob.getPriority());
                } else {
                    if (!tableClusterManager.cachedAndAdd(queryJob)) {
                        if (tableClusterManager.isFree()) {
                            log.debug("TableClusterManager is Free: [tableName=" + tableName + "][sql=" + queryJob.getSql() + "]");
                            tableClusterManager.query(queryJob);
                        } else {
                            log.debug("Connection is not Free, push back the job to queue");
                            jobQueue.push(queryJob, queryJob.getPriority());
                        }
                    }
                }
            }
        }
    }

    class ResultWorder extends JobWorker {
        public ResultWorder(String name) {
            super(name);
        }

        @Override
        public void doJob() {
            Job job = resultQueue.pop();
            if (job != null) {
                log.debug("Sending Result of job: [job=" + job + "]");
                QueryJob queryJob = (QueryJob)job;
                String callback = queryJob.getCallbackUrl();
                if (callback.startsWith("http")) {
                    doHttpResult(queryJob);
                } else if (callback.startsWith("file")) {
                    doFileResult(queryJob);
                } else {
                    doLogResult(queryJob);
                }
            }
        }

        private void doHttpResult(QueryJob queryJob) {
            HttpUtil.postQueryJob(queryJob);
        }

        private void doFileResult(QueryJob queryJob) {
            log.debug("Sending to File: [callback=" + queryJob.getCallbackUrl() + "]");
            try {
                URI fileUri = new URI(queryJob.getCallbackUrl());
                log.debug("fileUri=" + fileUri);
                File resultFile = new File(fileUri);
                if (!resultFile.exists()) {
                    resultFile.createNewFile();
                    log.debug("Created new file: [file=" + resultFile.getAbsolutePath() + "]");
                }
                FileWriter fileWriter = new FileWriter(resultFile, true);
                fileWriter.write(queryJob.toString());
                fileWriter.write(System.lineSeparator());
                fileWriter.close();
                log.debug("Write log to File: [path=" + resultFile.getAbsolutePath() + "]");

            } catch (URISyntaxException | IOException e) {
                log.error("Exception: ", e);
            }
        }

        private void doLogResult(QueryJob queryJob) {
            log.debug("Sending to Log: [callback=" + queryJob.getCallbackUrl() + "]");
            log.info("Result: " + queryJob);
        }
    }


}
