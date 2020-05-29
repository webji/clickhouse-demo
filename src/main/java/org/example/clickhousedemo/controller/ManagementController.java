package org.example.clickhousedemo.controller;

import org.example.clickhousedemo.cluster.config.Config;
import org.example.clickhousedemo.cluster.config.ConfigManager;
import org.example.clickhousedemo.cluster.db.DBClusterManager;
import org.example.clickhousedemo.cluster.db.HostConnectionManager;
import org.example.clickhousedemo.cluster.query.QueryManager;
import org.example.clickhousedemo.common.JobQueue;
import org.example.clickhousedemo.common.SQLCache;
import org.example.clickhousedemo.http.request.CacheRequestBody;
import org.example.clickhousedemo.http.request.ConfigRequestBody;
import org.example.clickhousedemo.http.ResponseMessage;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/management")
public class ManagementController {

    @PostMapping("/cache/enable")
    public ResponseMessage enableCache() {
        ConfigManager configManager = ConfigManager.getInstance();
        configManager.getConfig().setCacheEnabled(true);
        return ResponseMessage.success(configManager);
    }

    @PostMapping("/cache/disable")
    public ResponseMessage disableCache() {
        ConfigManager configManager = ConfigManager.getInstance();
        configManager.getConfig().setCacheEnabled(false);
        return ResponseMessage.success(configManager);
    }

    @PostMapping("/cache/clear")
    public ResponseMessage clearCache() {
        QueryManager queryManager = QueryManager.getInstance();
        SQLCache sqlCache = queryManager.getSqlCache();
        int count = sqlCache.clear();
        return ResponseMessage.success(count);
    }

    @GetMapping("/cache/count")
    public ResponseMessage countCache() {
        QueryManager queryManager = QueryManager.getInstance();
        SQLCache sqlCache = queryManager.getSqlCache();
        int count = sqlCache.count();
        return ResponseMessage.success(count);
    }

    @GetMapping("/cache/key/list")
    public ResponseMessage keyListCache() {
        QueryManager queryManager = QueryManager.getInstance();
        SQLCache sqlCache = queryManager.getSqlCache();
        List<String> keyList = sqlCache.keyList();
        return ResponseMessage.success(keyList);
    }

    @PostMapping("/cache/key")
    public ResponseMessage getCacheOfKey(@RequestBody CacheRequestBody requestBody) {
        QueryManager queryManager = QueryManager.getInstance();
        SQLCache sqlCache = queryManager.getSqlCache();
        return ResponseMessage.success(sqlCache.get(requestBody.getKey()));
    }

    @GetMapping("/queue/query/count")
    public ResponseMessage countQueryQueue() {
        QueryManager queryManager = QueryManager.getInstance();
        JobQueue jobQueue = queryManager.getJobQueue();
        return ResponseMessage.success(jobQueue.count());
    }

    @GetMapping("/queue/query/list")
    public ResponseMessage listQueryQueue() {
        QueryManager queryManager = QueryManager.getInstance();
        JobQueue jobQueue = queryManager.getJobQueue();
        return ResponseMessage.success(jobQueue.jobMap());
    }

    @GetMapping("/queue/result/count")
    public ResponseMessage countResultQueue() {
        QueryManager queryManager = QueryManager.getInstance();
        JobQueue resultQueue = queryManager.getResultQueue();
        return ResponseMessage.success(resultQueue.count());
    }

    @GetMapping("/queue/result/list")
    public ResponseMessage listResultQueue() {
        QueryManager queryManager = QueryManager.getInstance();
        JobQueue resultQueue = queryManager.getResultQueue();
        return ResponseMessage.success(resultQueue.jobMap());
    }

    @PostMapping("/connection/max")
    public ResponseMessage setConnectionMax(@RequestBody ConfigRequestBody requestBody) {
        ConfigManager configManager = ConfigManager.getInstance();
        if (requestBody.getConnectionMax() != null) {
            configManager.getConfig().setConnectionMax(requestBody.getConnectionMax());
        }
        return ResponseMessage.success(configManager.getConfig());
    }

    @GetMapping("/connection/list")
    public ResponseMessage getConnections() {
        DBClusterManager dbClusterManager = DBClusterManager.getInstance();
        ConfigManager configManager = ConfigManager.getInstance();
        int connectionMax = configManager.getConfig().getConnectionMax();

        Map<String, Map<String, Integer>> connectionsMap = new HashMap<>();
        for (HostConnectionManager hostConnectionManager : dbClusterManager.getHostConnectionManagerMap().values()) {
            Map<String, Integer> connectionMap = new HashMap<>();
            int idle = hostConnectionManager.getIdleConnections().size();
            int busy =  hostConnectionManager.getBusyConnections().size();
            int gap = connectionMax -idle - busy;
            connectionMap.put("idle", idle);
            connectionMap.put("busy", busy);
            connectionMap.put("gap", gap);
            connectionsMap.put(hostConnectionManager.getIp(), connectionMap);
        }
        return ResponseMessage.success(connectionsMap);
    }


    @GetMapping("/config")
    public ResponseMessage getConfig() {
        ConfigManager configManager = ConfigManager.getInstance();
        return ResponseMessage.success(configManager.getConfig());
    }

    @PostMapping("/config")
    public ResponseMessage updateConfig(@RequestBody ConfigRequestBody requestBody) {
        Config config = ConfigManager.getInstance().getConfig();
        if (requestBody.getConnectionMax() != null) {
            config.setConnectionMax(requestBody.getConnectionMax());
        }
        if (requestBody.getDummyDelay() != null) {
            config.setDummyDelay(requestBody.getDummyDelay());
        }
        if (requestBody.getCacheEnabled() != null) {
            config.setCacheEnabled(requestBody.getCacheEnabled());
        }
        return ResponseMessage.success(config);
    }


}
