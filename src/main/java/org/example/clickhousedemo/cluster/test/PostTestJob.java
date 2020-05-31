package org.example.clickhousedemo.cluster.test;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.example.clickhousedemo.cluster.config.Config;
import org.example.clickhousedemo.cluster.config.ConfigManager;
import org.example.clickhousedemo.cluster.query.QueryJob;
import org.example.clickhousedemo.common.JobWorker;
import org.example.clickhousedemo.http.request.QueryRequestBody;
import org.example.clickhousedemo.http.request.TestRequestBody;
import org.example.clickhousedemo.util.HttpUtil;
import org.example.clickhousedemo.util.JsonUtil;
import org.example.clickhousedemo.util.ThreadUtil;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Data
public class PostTestJob extends AbstractTestJob {
    Config config;
    PoolingHttpClientConnectionManager connectionManager;



    public PostTestJob(TestRequestBody testConfig) {
        super(testConfig);
        config = ConfigManager.getInstance().getConfig();
        connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(testConfig.getThreadNum() * testConfig.getRepeat() * 80 + 1);
        connectionManager.setDefaultMaxPerRoute(testConfig.getThreadNum() * testConfig.getRepeat() * 40);
    }

    public JobWorker getWorker(int i) {
        String name = "PostTestWorker-" + getName() + "-" + i;
        return new PostTestJobWorker(name);
    }


    class PostTestJobWorker extends JobWorker {

        CloseableHttpClient httpClient;
        List<QueryRequestBody> requestList;
        HttpPost post;
        Integer repeat;

        public PostTestJobWorker(String name) {
            super(name);
            requestList = new ArrayList<>();
            for (QueryRequestBody requestBody : queryList) {
                requestList.add(requestBody);
            }
            httpClient = HttpClientBuilder.create()
                    .setConnectionManager(connectionManager)
                    .build();
            post = new HttpPost(config.getQueryRequestUrl());
            repeat = 0;
        }

        private void post(QueryRequestBody requestBody) {
            post.setEntity(new StringEntity(JsonUtil.stringOfObject(requestBody), Charset.forName("utf-8")));
            post.addHeader("Content-Type", "application/json");
            try {
                log.debug("Post: request=" + requestBody);
                HttpResponse response = httpClient.execute(post);
                log.debug("HTTP Response: " + response.getEntity().getContent().toString());
            } catch (ClientProtocolException e) {
                log.error("Exception: ", e);
            } catch (IOException e) {
                log.error("Exception: ", e);
            }
        }

        @Override
        public void doJob() {
            for (QueryRequestBody requestBody : requestList) {
                post(requestBody);
                if (testConfig.getItemDelay() != null) {
                    ThreadUtil.sleep(testConfig.getItemDelay());
                }
            }
            if (testConfig.getTotalDelay() != null) {
                ThreadUtil.sleep(testConfig.getTotalDelay());
            }
            repeat++;
            if (repeat >= testConfig.getRepeat()) {
                pause();
            }
        }
    }

}
