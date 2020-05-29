package org.example.clickhousedemo.http.request;

import lombok.Data;

import java.io.Serializable;

@Data
public class ConfigRequestBody implements Serializable {
    Boolean cacheEnabled;
    Integer connectionMax;
    Integer dummyDelay;
}
