package org.example.clickhousedemo.controller;

import org.example.clickhousedemo.util.ClickHouseUtil;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class QueryController {

    @GetMapping("/query")
    public List<Map<String, Object>> query(@RequestParam(value = "sql", defaultValue = "") String sql) {
        return ClickHouseUtil.sqlQuery(sql);
    }
}
