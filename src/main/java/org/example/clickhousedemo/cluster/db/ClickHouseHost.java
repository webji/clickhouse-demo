package org.example.clickhousedemo.cluster.db;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.clickhousedemo.cluster.config.Config;
import org.example.clickhousedemo.cluster.config.ConfigManager;
import org.example.clickhousedemo.util.dummy.DummyDataSource;
import org.springframework.util.StringUtils;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

@Slf4j
@Data
@NoArgsConstructor
public class ClickHouseHost implements Serializable {
    String hostname;
    String ip;
    Integer port;
    String username;
    String password;
    String db;
    Integer socketTimeout = 0;

    List<String> tables;

    @JsonIgnore
    DataSource dataSource;

    @JsonIgnore
    String url;

    public void initialize() {
        Config config = ConfigManager.getInstance().getConfig();
        ClickHouseProperties properties = new ClickHouseProperties();
        if (!StringUtils.isEmpty(ip) && port != null) {
            url = "jdbc:clickhouse://" + ip + ":" + port;
        }
        if (!StringUtils.isEmpty(username)) {
            properties.setUser(username);
        }
        if (!StringUtils.isEmpty(password)) {
            properties.setPassword(password);
        }
        if (!StringUtils.isEmpty(db)) {
            properties.setDatabase(db);
        }
        if (socketTimeout != null) {
            properties.setSocketTimeout(socketTimeout);
        }
        if (StringUtils.isEmpty(url)) {
            log.error("Failed, url is Empty");
            return;
        }
        if (config.getDummy()) {
            dataSource = new DummyDataSource();
        } else {
            dataSource = new ClickHouseDataSource(url, properties);
        }
    }

    public Connection connect() {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            log.error("Exception: ", e);
        }
        return connection;
    }

    private void initializeDB() {
        String sql = "show databases";
        Connection connection = connect();
        if (connection != null) {
            try {
                Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery(sql);

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
}
