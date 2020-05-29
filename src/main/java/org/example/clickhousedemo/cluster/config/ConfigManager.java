package org.example.clickhousedemo.cluster.config;

import lombok.Data;
import org.example.clickhousedemo.util.JsonUtil;

import java.io.Serializable;

@Data
public class ConfigManager implements Serializable {
    private static ConfigManager instance;

    private static String CONFIG_JSON = "config.json";

    private Config config;

    private ConfigManager() {
        initialize();
    }

    public static synchronized ConfigManager getInstance() {
        if (instance == null) {
            instance = new ConfigManager();
        }
        return instance;
    }

    private void initialize() {
        config = JsonUtil.objectOfResourceFile(CONFIG_JSON, Config.class);
        if (config == null) {
            config = new Config();
        }
    }



}
