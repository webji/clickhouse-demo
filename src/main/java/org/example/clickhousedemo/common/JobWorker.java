package org.example.clickhousedemo.common;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.example.clickhousedemo.common.Worker;

@Slf4j
@Data
public abstract class JobWorker implements Worker, Runnable {
    String name;
    boolean running = true;
    boolean shouldQuit = false;
    Object lock;

    public JobWorker(String name) {
        this.name = name;
        lock = new Object();
    }

    public String getName() {
        return this.name;
    }

    private void lockWait() {
        synchronized (lock) {
            try {
                lock.wait();
            } catch (InterruptedException e) {
                log.error("Exception: ", e);
            }
        }
    }

    private void lockNotify() {
        synchronized (lock) {
            try {
                lock.notify();
            } catch (Exception e) {
                log.error("Exception: ", e);
            }
        }
    }

    public void start() {
        shouldQuit = false;
        this.running = true;
    }
    public void pause() {
        this.running = false;
    }

    public void resume() {
        this.running = true;
        lockNotify();
    }

    public void stop() {
        shouldQuit = true;
        running = false;
        lockNotify();
    }

    public void doJob(){}

    @Override
    public void run() {
        Thread.currentThread().setName(name);
        while (shouldQuit == false) {
            if (this.running == true) {
                doJob();
            } else {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    log.error("Exception: ", e);
                }
                log.debug("Work is Dummy Running: [running=" + this.running + "]");
                lockWait();
            }
        }
        log.debug("Worker is Quit: [shouldQuit=" + this.shouldQuit + "]");
    }
}
