#Springboot ClickHouse Daemon

## Setup

### Setup Server

```bash
docker pull yandex/clickhouse-server
docker pull yandex/clickhouse-client

docker run -d --name chs.192.168.0.40 --net nexus --ip 192.168.0.40 --ulimit nofile=262144:262144 yandex/clickhouse-server
docker run -it --rm --network nexus --ip 192.168.0.41 --link chs.192.168.0.40:clickhouse-server yandex/clickhouse-client --host clickhouse-server

curl https://clickhouse-datasets.s3.yandex.net/hits/tsv/hits_v1.tsv.xz | unxz --threads=`nproc` > hits_v1.tsv
curl https://clickhouse-datasets.s3.yandex.net/visits/tsv/visits_v1.tsv.xz | unxz --threads=`nproc` > visits_v1.tsv

docker cp ./hits_v1.tsv chs.192.168.0.40:/tmp
docker cp ./visits_v1.tsv chs.192.168.0.40:/tmp

clickhouse-client --query "CREATE DATABASE IF NOT EXISTS tutorial"

Flow the guide in the tutorial to create db and initialize data
https://clickhouse.tech/docs/en/getting_started/tutorial/

```

## Test

### Build App Docker

```bash
docker build -t clickhouse-daemon .
docker run -it --rm -p 8080:8080 -d clickhouse-daemon
```

### Access via Web
http://localhost:32773/query?sql=SELECT COUNT(*) FROM tutorial.hits_v1

```sql
SELECT COUNT(*) FROM tutorial.hits_v1

SELECT COUNT(*) FROM tutorial.visits_v1

SELECT
    StartURL AS URL,
    AVG(Duration) AS AvgDuration
FROM tutorial.visits_v1
WHERE StartDate BETWEEN '2014-03-23' AND '2014-03-30'
GROUP BY URL
ORDER BY AvgDuration DESC
LIMIT 10

```