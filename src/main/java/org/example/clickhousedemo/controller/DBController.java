package org.example.clickhousedemo.controller;

import org.example.clickhousedemo.cluster.db.DBClusterManager;
import org.example.clickhousedemo.http.ResponseMessage;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/db")
public class DBController {

    @GetMapping("/")
    public ResponseMessage listDB() {
        DBClusterManager dbClusterManager = DBClusterManager.getInstance();
        return ResponseMessage.success(dbClusterManager.getDbCluster());
    }
}
