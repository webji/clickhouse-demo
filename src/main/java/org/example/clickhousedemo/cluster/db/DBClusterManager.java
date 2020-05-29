package org.example.clickhousedemo.cluster.db;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.example.clickhousedemo.util.JsonUtil;

import java.util.*;

@Slf4j
@Data
public class DBClusterManager {
    private static DBClusterManager instance;

    private static String CLUSTER_JSON = "cluster.json";
    DBCluster dbCluster;
    Map<String, HostConnectionManager> hostConnectionManagerMap = new HashMap<>();
    Set<String> tableNameSet = new HashSet<>();

    private DBClusterManager() {
        initialize();
    }

    public static synchronized DBClusterManager getInstance() {
        if (instance == null) {
            instance = new DBClusterManager();
        }
        return instance;
    }

    public void initialize() {
        dbCluster = JsonUtil.objectOfResourceFile(CLUSTER_JSON, DBCluster.class);
        dbCluster.initialize();
        for (ClickHouseHost host : dbCluster.getHosts()) {
            HostConnectionManager hostConnectionManager = new HostConnectionManager(host);
            hostConnectionManagerMap.put(hostConnectionManager.getIp(), hostConnectionManager);
        }
    }

    public void stop() {
        for (HostConnectionManager hostConnectionManager : hostConnectionManagerMap.values()) {
            hostConnectionManager.stop();
        }
    }

}
