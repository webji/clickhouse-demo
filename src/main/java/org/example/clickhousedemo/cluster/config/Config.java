package org.example.clickhousedemo.cluster.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class Config implements Serializable {
    Boolean cacheEnabled = true;
    Integer connectionMax = 20;
    Boolean dummy = true;
    Integer dummyDelay = 120;
}
