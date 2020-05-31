package org.example.clickhousedemo.http.request;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

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

    public static QueryRequestBody fromMap(Map<String, Object> requestMap) {
        QueryRequestBody requestBody = new QueryRequestBody();
        requestBody.setCallback((String)requestMap.get("callback"));
        requestBody.setTableName((String)requestMap.get("tableName"));
        requestBody.setPriority((String)requestMap.get("priority"));
        requestBody.setSync((Boolean)requestMap.get("sync"));
        requestBody.setSql((String)requestMap.get("sql"));
        return requestBody;
    }
}
