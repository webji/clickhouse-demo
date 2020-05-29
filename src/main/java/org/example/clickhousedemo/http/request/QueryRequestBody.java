package org.example.clickhousedemo.http.request;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Data
public class QueryRequestBody implements Serializable {
    String sql;
    Boolean sync = true;

    /**
     * High, Medium, Low
     */
    String priority = "MEDIUM";
    String tableName;

    String callback;
}
