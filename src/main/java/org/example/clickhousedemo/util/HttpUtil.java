package org.example.clickhousedemo.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.example.clickhousedemo.cluster.query.QueryJob;

import java.io.IOException;
import java.nio.charset.Charset;

@Slf4j
public class HttpUtil {
    public static void postQueryJob(QueryJob queryJob) {
        log.debug("Sending to HTTP: [callback=" + queryJob.getCallbackUrl() + "]");
        String callback = queryJob.getCallbackUrl();
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(callback);
        post.setEntity(new StringEntity(JsonUtil.stringOfObject(queryJob.toRequest()), Charset.forName("utf-8")));
        post.addHeader("Content-Type", "application/json");
        try {
            HttpResponse response = httpClient.execute(post);
            log.debug("HTTP Response: " + response.getEntity().getContent().toString());
        } catch (ClientProtocolException e) {
            log.error("Exception: ", e);
        } catch (IOException e) {
            log.error("Exception: ", e);
        }
    }
}
