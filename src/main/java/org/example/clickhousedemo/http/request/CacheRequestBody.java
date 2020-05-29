package org.example.clickhousedemo.http.request;

import lombok.Data;

import java.io.Serializable;

@Data
public class CacheRequestBody implements Serializable {
    String key;
}
