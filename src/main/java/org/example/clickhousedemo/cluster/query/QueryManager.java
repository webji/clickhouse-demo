package org.example.clickhousedemo.cluster.query;

import com.google.common.collect.Sets;
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

    /**
     * All Supported Tables
     */
    Set<String> tableNameSet = Sets.newConcurrentHashSet();

    /**
     * TableClusterManager binded to Table
     */
    Map<String, TableClusterManager> tableClusterManagerMap = new HashMap<>();

    SQLCache sqlCache = new MemorySQLCache();

    ExecutorService threadExecutor;

    ResultWorder resultWorder;

    Config config;

    private QueryManager() {
        initialize();
        threadExecutor = Executors.newSingleThreadExecutor();
        resultWorder = new ResultWorder("ResultWorker");
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
                tableClusterManager = new TableClusterManager(tableName);
                tableClusterManager.setResultQueue(resultQueue);
                tableClusterManager.setSqlCache(sqlCache);
                tableClusterManager.setConfig(config);
                tableClusterManagerMap.put(tableName, tableClusterManager);
            }
            for (HostConnectionManager hostConnectionManager : dbClusterManager.getHostConnectionManagerMap().values()) {
                if (hostConnectionManager.hasTable(tableName)) {
                    tableClusterManager.addHostConnectionManager(hostConnectionManager);
                }
            }
        }
    }



    public synchronized ResponseMessage query(QueryJob job) {
        log.debug("Query: " + job);
        boolean cacheEnabled = config.getCacheEnabled();
        boolean cacheHit = false;
        String sql = job.getSql();

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
            return ResponseMessage.success(job);
        } else {
            String tableName = job.getTableName();
            log.debug("Query for tableName=" + tableName);
            if (!tableNameSet.contains(tableName)) {
                return ResponseMessage.fail("Table Not Existed: [tableName=" + tableName + "]", job);
            }
            log.debug("Try to find tableClusterManager for tableName" + tableName);
            TableClusterManager tableClusterManager = tableClusterManagerMap.get(tableName);
            log.debug("Found TableClusterManager for Job: [TableClusterManager=" + tableClusterManager + "][job=" + job + "]");
            return tableClusterManager.query(job);
        }
    }


    class ResultWorder extends JobWorker {
        public ResultWorder(String name) {
            super(name);
        }

        @Override
        public void doJob() {
            Job job;
            while ((job = resultQueue.pop()) != null) {
                log.debug("Sending Result of job: [job=" + job + "]");
                job.setStatus(QueryJobStatus.SENDING);
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
            resultQueue.lockWait();
        }

        private void doHttpResult(QueryJob queryJob) {
            HttpUtil.postQueryJob(queryJob);
        }

        private void doFileResult(QueryJob queryJob) {
            log.debug("Sending to File: [callback=" + queryJob.getCallbackUrl() + "], job=" + queryJob);
            try {
                URI fileUri = new URI(queryJob.getCallbackUrl());
                File resultFile = new File(fileUri);
                if (!resultFile.exists()) {
                    resultFile.createNewFile();
                    log.debug("Created new file: [file=" + resultFile.getAbsolutePath() + "]");
                }
                FileWriter fileWriter = new FileWriter(resultFile, true);
                fileWriter.write(queryJob.toString());
                fileWriter.write(System.lineSeparator());
                fileWriter.close();
                log.debug("Write log to File: [path=" + resultFile.getAbsolutePath() + "][job=" + queryJob + "]");

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
