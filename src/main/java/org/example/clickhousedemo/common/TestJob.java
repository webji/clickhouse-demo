package org.example.clickhousedemo.common;

public interface TestJob {
    void setName(String name);
    String getName();
    void start();
    void pause();
    void resume();
    void stop();
}
