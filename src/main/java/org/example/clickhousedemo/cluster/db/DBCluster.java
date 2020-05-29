package org.example.clickhousedemo.cluster.db;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Data
@NoArgsConstructor
public class DBCluster implements Serializable {
    List<ClickHouseHost> hosts;

    public void initialize() {
        for (ClickHouseHost host : hosts) {
            host.initialize();
        }
    }

    public List<ClickHouseHost> getClickHouseHostFromTable(String table) {
        List<ClickHouseHost> ret = new ArrayList<>();
        for (ClickHouseHost host : hosts) {
            if (host.getTables().contains(table)) {
                log.debug("Found [tableName=" + table + "][host:" + host + "]");
                ret.add(host);
            }
        }
        return ret;
    }
}
