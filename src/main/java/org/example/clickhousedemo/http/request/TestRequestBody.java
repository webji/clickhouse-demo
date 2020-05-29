package org.example.clickhousedemo.http.request;

import lombok.Data;

import java.io.Serializable;

@Data
public class TestRequestBody implements Serializable {
    /**
     * HTTP/QUEUE
     */
    String type;
    String fileName;
    String filePath;
    Integer repeat;
    Integer itemDelay;
    Integer totalDelay;
    Integer threadNum;
}
