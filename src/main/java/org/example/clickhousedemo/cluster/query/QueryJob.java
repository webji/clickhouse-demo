package org.example.clickhousedemo.cluster.query;


import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.ToString;
import org.example.clickhousedemo.cluster.db.TableClusterManager;
import org.example.clickhousedemo.common.Job;
import org.example.clickhousedemo.http.request.QueryRequestBody;
import org.example.clickhousedemo.util.DateTimeUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@ToString(exclude = {"tableClusterManager"})
public class QueryJob implements Job, Serializable {
    Integer id;
    JobPriority priority;
    QueryJobStatus status;
    Boolean sync;
    String sender;
    String callbackUrl;
    String tableName;
    String sql;
    String result;
    List<String> logs = new ArrayList<>();

    @JsonIgnore
    TableClusterManager tableClusterManager;

    public void setStatus(QueryJobStatus status) {
        if (this.status != status) {
            logs.add("[" + DateTimeUtil.currentDateString() + "] : [" + this.status + "] ==> [" + status + "]");
            this.status = status;
        }
    }

    public static QueryJob fromRequest(QueryRequestBody requestBody) {
        JobPriority priority = JobPriority.fromName(requestBody.getPriority());
        QueryJob queryJob = new QueryJob();
        queryJob.setSync(requestBody.getSync());
        queryJob.setTableName(requestBody.getTableName());
        queryJob.setPriority(priority);
        queryJob.setSql(requestBody.getSql());
        queryJob.setCallbackUrl(requestBody.getCallback());
        queryJob.setStatus(QueryJobStatus.RECEIVED);
        return queryJob;
    }

    public QueryRequestBody toRequest() {
        QueryRequestBody requestBody = new QueryRequestBody();
        requestBody.setTableName(tableName);
        requestBody.setPriority(priority.name());
        requestBody.setSync(sync);
        requestBody.setSql(sql);
        requestBody.setCallback(callbackUrl);
        return requestBody;
    }

}
