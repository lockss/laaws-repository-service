FROM openjdk:8-jre

MAINTAINER "Daniel Vargas" <dlvargas@stanford.edu>

EXPOSE 24610

ARG JAR_FILE

WORKDIR /opt/lockss

ENTRYPOINT ["/init.sh"]

# Install netcat and add entrypoint script
RUN apt update && apt -y install netcat
ADD scripts/init.sh /init.sh

# Add the LOCKSS repository JAR and entrypoint script
ADD ${JAR_FILE} /opt/lockss/spring-app.jar
