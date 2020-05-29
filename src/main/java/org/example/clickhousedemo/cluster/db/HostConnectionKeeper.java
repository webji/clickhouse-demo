package org.example.clickhousedemo.cluster.db;

import lombok.extern.slf4j.Slf4j;
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
    ConcurrentLinkedQueue<QueryJob> jobQueue;
    ConnectionWorker worker;


    public HostConnectionKeeper(Integer i, Connection connection, HostConnectionManager hostConnectionManager) {
        this.connection = connection;
        this.hostConnectionManager = hostConnectionManager;
        this.name = "HostConnKeeper-" + i;
        queryThread = Executors.newSingleThreadExecutor();
        jobQueue = new ConcurrentLinkedQueue<>();
        worker = new ConnectionWorker(name + "Worker", this);
        queryThread.submit(worker);
    }

    public void query(QueryJob job) {
        jobQueue.offer(job);
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
            QueryJob job = jobQueue.poll();
            if (job != null) {
                try {
                    job.setStatus(QueryJobStatus.QUERYING);
                    log.debug("Start Statement [sql=" + job.getSql() + "]");
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(job.getSql());
                    job.setResult(resultSet.toString());
                    hostConnectionManager.compelte(hostConnectionKeeper, job);
                } catch (SQLException e) {
                    log.error("Exception: ", e);
                }
            }
        }
    }
}
