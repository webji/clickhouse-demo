package org.example.clickhousedemo.util;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ThreadUtil {
    public static void sleep(Integer s) {
        try {
            Thread.sleep(s * 1000);
        } catch (InterruptedException e) {
            log.error("Exception: ", e);
        }
    }
}
