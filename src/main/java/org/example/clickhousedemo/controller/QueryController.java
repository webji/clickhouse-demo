package org.example.clickhousedemo.controller;

import lombok.extern.slf4j.Slf4j;
import org.example.clickhousedemo.cluster.query.JobPriority;
import org.example.clickhousedemo.cluster.query.QueryJob;
import org.example.clickhousedemo.cluster.query.QueryJobStatus;
import org.example.clickhousedemo.cluster.query.QueryManager;
import org.example.clickhousedemo.http.request.QueryRequestBody;
import org.example.clickhousedemo.http.ResponseMessage;
import org.example.clickhousedemo.util.ClickHouseUtil;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/query")
public class QueryController {

    @GetMapping("/")
    public List<Map<String, Object>> query(@RequestParam(value = "sql", defaultValue = "") String sql) {
        return ClickHouseUtil.sqlQuery(sql);
    }

    @PostMapping("/")
    public ResponseMessage postQuery(@RequestBody QueryRequestBody requestBody) {
        QueryManager queryManager = QueryManager.getInstance();
        QueryJob queryJob = QueryJob.fromRequest(requestBody);
        queryJob.setStatus(QueryJobStatus.INITIALIZED);
        return queryManager.query(queryJob);
    }

    @PostMapping("/result")
    public ResponseMessage postResult(@RequestBody QueryJob queryJob) {
        queryJob.setStatus(QueryJobStatus.COMPLETED);
        log.debug("Received Result: " + queryJob);
        return ResponseMessage.success();
    }
}
