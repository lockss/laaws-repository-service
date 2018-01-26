FROM openjdk:8
MAINTAINER "Daniel Vargas" <dlvargas@stanford.edu>

# Install Maven and ensure all packages are up to date
RUN apt update && apt -y install maven

# Create a directory for the repository and tools
RUN mkdir /laaws-repository
WORKDIR /laaws-repository

# Add source code and compile
ADD src src
ADD pom.xml pom.xml
RUN mvn package

# Add the WARC importer script
ADD docker/bin/warcimporter .
ENV PATH="/laaws-repository:${PATH}"

# Extract Java libs for WARC importer 
RUN unzip target/laaws-repository-1.0.0.jar && rm -rf META-INF org && mv BOOT-INF lib
ADD docker/lib/spring-core-4.1.0.RELEASE.jar lib/lib/spring-core-4.1.0.RELEASE.jar

# Default repository port
EXPOSE 8080

# Set the entrypoint to launch the Repository Service
CMD ["mvn", "spring-boot:run"]
