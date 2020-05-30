package org.example.clickhousedemo.cluster.db;

import lombok.extern.slf4j.Slf4j;
import org.example.clickhousedemo.cluster.query.MemoryJobQueue;
import org.example.clickhousedemo.cluster.query.QueryJob;
import org.example.clickhousedemo.cluster.query.QueryJobStatus;
import org.example.clickhousedemo.common.JobWorker;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class HostConnectionKeeper {
    String name;

    Connection connection;
    HostConnectionManager hostConnectionManager;

    ExecutorService queryThread;
    MemoryJobQueue jobQueue;
    ConnectionWorker worker;


    public HostConnectionKeeper(Integer i, Connection connection, HostConnectionManager hostConnectionManager) {
        this.connection = connection;
        this.hostConnectionManager = hostConnectionManager;
        this.name = "HostConnKeeper-" + i;
        queryThread = Executors.newSingleThreadExecutor();
        jobQueue = new MemoryJobQueue();
        worker = new ConnectionWorker(name + "Worker", this);
        queryThread.submit(worker);
    }

    public void asyncQuery(QueryJob job) {
        log.debug("Start Async Job, Add to jobQueue" + job);
        jobQueue.push(job, job.getPriority());
    }

    public void syncQuery(QueryJob job) {
        if (job != null) {
            log.debug("Start Sync Job: " + job);
            try {
                job.setStatus(QueryJobStatus.QUERYING);
                log.debug("Start Statement [job=" + job + "]");
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(job.getSql());
                job.setResult(resultSet.toString());
                log.debug("Complete Statement: " + job);
            } catch (SQLException e) {
                log.error("Exception: ", e);
                job.setResult("ERROR");
            }
            log.debug("Completed Sync Job Query, " + job);
            hostConnectionManager.compelte(this, job);
        }
    }

    public void stop() {
        worker.stop();
    }

    class ConnectionWorker extends JobWorker {
        private HostConnectionKeeper hostConnectionKeeper;
        public ConnectionWorker(String name, HostConnectionKeeper hostConnectionKeeper) {
            super(name);
            this.hostConnectionKeeper = hostConnectionKeeper;
        }

        @Override
        public void doJob() {
            QueryJob job;
            while((job = (QueryJob)jobQueue.pop()) != null) {
                syncQuery(job);
            }
            jobQueue.lockWait();
        }
    }
}
