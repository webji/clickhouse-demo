package org.example.clickhousedemo.cluster.db;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.example.clickhousedemo.cluster.query.QueryJob;
import org.example.clickhousedemo.cluster.config.Config;
import org.example.clickhousedemo.cluster.config.ConfigManager;
import org.example.clickhousedemo.common.JobWorker;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Data
public class HostConnectionManager {
    String ip;
    @ToString.Exclude
    ClickHouseHost clickHouseHost;
    @ToString.Exclude
    List<HostConnectionKeeper> idleConnections = new ArrayList<>();
    @ToString.Exclude
    List<HostConnectionKeeper> busyConnections = new ArrayList<>();

    @ToString.Exclude
    @JsonIgnore
    ExecutorService connectionThread;

    /**
     * sql - HostConnectionKeeper
     */
    @ToString.Exclude
    Map<String, HostConnectionKeeper> sqlConnectionMap = new HashMap<>();
    @ToString.Exclude
    Set<String> tableNameSet = new HashSet<>();
    @ToString.Exclude
    Config config;

    public HostConnectionManager(ClickHouseHost clickHouseHost) {
        this.clickHouseHost = clickHouseHost;
        this.ip = clickHouseHost.getIp();
        tableNameSet.addAll(clickHouseHost.getTables());
        config = ConfigManager.getInstance().getConfig();
        for (int i = 0; i < config.getConnectionMax(); i++) {
            Connection connection= clickHouseHost.connect();
            HostConnectionKeeper hostConnectionKeeper = new HostConnectionKeeper(i, connection, this);
            idleConnections.add(hostConnectionKeeper);
        }
    }

    public boolean hasTable(String tableName) {
        return tableNameSet.contains(tableName);
    }

    public Integer currentConnections() {
        return busyConnections.size();
    }

    public Integer availableConnections() {
        return config.getConnectionMax() - busyConnections.size();
    }

    public void query(QueryJob job) {
        HostConnectionKeeper hostConnectionKeeper = idleConnections.remove(0);
        busyConnections.add(hostConnectionKeeper);
        log.debug("Start Query, Move connectoin from IDLE to BUSY, [connection=" + hostConnectionKeeper.connection + "]");
        if (job.getSync()) {
            hostConnectionKeeper.syncQuery(job);
        } else {
            hostConnectionKeeper.asyncQuery(job);
        }
    }

    public boolean isFree() {
        return !(idleConnections.isEmpty() && idleConnections.size() + busyConnections.size() >= config.getConnectionMax());
    }

    public void compelte(HostConnectionKeeper hostConnectionKeeper, QueryJob job) {
        busyConnections.remove(hostConnectionKeeper);
        idleConnections.add(hostConnectionKeeper);
        log.debug("Finished Query [" + job.getId() + "], Move connection from BUSY to IDLE, [connection=" + hostConnectionKeeper.connection + "]");
        TableClusterManager tableClusterManager = job.getTableClusterManager();
        tableClusterManager.complete(job);
    }

    public void stop() {
        for (HostConnectionKeeper hostConnectionKeeper : idleConnections) {
            hostConnectionKeeper.stop();
        }
        for (HostConnectionKeeper hostConnectionKeeper : busyConnections) {
            hostConnectionKeeper.stop();
        }
    }

}
