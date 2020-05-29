package org.example.clickhousedemo.cluster.query;

public enum QueryJobStatus {
    RECEIVED("Received", 0),
    INITIALIZED("Initialized", 1),
    QUEUED("Queued", 2),
    QUERYING("Querying", 3),
    RESULTQUEUE("ResultQueue", 4),
    SENDING("Sending", 5),
    COMPLETED("Completed", 6);

    private QueryJobStatus(String name, Integer value) {
        this.name = name;
        this.value = value;
    }

    private String name;
    private Integer value;
}
