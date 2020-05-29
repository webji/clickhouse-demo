FROM maven:3.6-jdk-8-slim

COPY ./ /opt/clickhouse-demo
WORKDIR /opt/clickhouse-demo
RUN mkdir /root/.m2

VOLUME /root/.m2

#RUN mvn clean install

EXPOSE 8080

CMD mvn spring-boot:run