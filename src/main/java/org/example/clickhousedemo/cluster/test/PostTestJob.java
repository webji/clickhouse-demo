package org.example.clickhousedemo.cluster.test;

import lombok.Data;
import org.example.clickhousedemo.cluster.query.QueryJob;
import org.example.clickhousedemo.common.JobWorker;
import org.example.clickhousedemo.http.request.QueryRequestBody;
import org.example.clickhousedemo.http.request.TestRequestBody;
import org.example.clickhousedemo.util.HttpUtil;
import org.example.clickhousedemo.util.ThreadUtil;

import java.util.ArrayList;
import java.util.List;

@Data
public class PostTestJob extends AbstractTestJob {


    public PostTestJob(TestRequestBody testConfig) {
        super(testConfig);
    }

    public JobWorker getWorker(int i) {
        String name = "PostTestWorker-" + getName() + "-" + i;
        return new PostTestJobWorker(name);
    }


    class PostTestJobWorker extends JobWorker {
        List<QueryJob> jobList;
        public PostTestJobWorker(String name) {
            super(name);
            jobList = new ArrayList<>();
            for (QueryRequestBody requestBody : queryList) {
                QueryJob queryJob = QueryJob.fromRequest(requestBody);
                jobList.add(queryJob);
            }
        }

        @Override
        public void doJob() {
            for (QueryJob queryJob : jobList) {
                HttpUtil.postQueryJob(queryJob);
                if (testConfig.getItemDelay() != null) {
                    ThreadUtil.sleep(testConfig.getItemDelay());
                }
            }
            if (testConfig.getTotalDelay() != null) {
                ThreadUtil.sleep(testConfig.getTotalDelay());
            }
        }
    }

}
