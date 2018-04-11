FROM openjdk:8-jre

MAINTAINER "Daniel Vargas" <dlvargas@stanford.edu>

ENTRYPOINT ["/usr/bin/java", "-jar", "/opt/lockss/spring-app.jar"]

EXPOSE 32640

ARG JAR_FILE

WORKDIR /opt/lockss

ADD ${JAR_FILE} /opt/lockss/spring-app.jar
