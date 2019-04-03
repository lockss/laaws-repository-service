# Introduction

# Compiling
## Dependencies
## Docker image

# Running
## Locally
## Within Docker

# Examples
## Ingesting existing WARC files
## Add an artifact
## Retrieve an artifact
## Repository queries

# Importing WARCs with WARCImporter
The WARCImporter tool is now included in the Docker image created by the Dockerfile in 
this project. To use it follow these steps:

1. Build the `laaws/laaws-repository` Docker image:

    `docker build -t laaws/laaws-repository .`
    
1. Invoke the WARCImporter using the `warcimporter` wrapper script:

    `docker run --rm -v $WARCSRC:/warcs laaws/laaws-repository warcimporter --host $HOST--repo $REPO --auid $AUID WARC1.warc ... WARCN.warc`

    Make sure to replace the variables with the proper settings for your environment:
    * `WARCSRC`: Set this to the absolute path of the directory containing WARC files on your workstation
    or host system.
    * `HOST`: Set this to the base URL of a LAAWS Repository Service instance (e.g., `http://localhost:8080/`)
    * `REPO`: Set this to the name of the repository you wish to add the WARC files to.
    * `AUID`: Set this to the AUID you wish the WARC files to be added to.
    
    Please note that while more than one WARC file may be specified, their WARC records will share the same
    repository and AUID settings.

# Configuring OpenWayback to use this service for indexing and replay
  Assuming that the LAAWS Repository service is running on host `reposervicehost` at port `reposerviceport` and it has a collection `collectionid`:
  
  * Edit the file `wayback-webapp/src/main/webapp/WEB-INF/wayback.xml`:
    1. Comment out the `resourcefilelocationdb` bean:

              <!--  
                <bean id="resourcefilelocationdb" class="org.archive.wayback.resourcestore.locationdb.BDBResourceFileLocationDB">  
                  <property name="bdbPath" value="${wayback.basedir}/file-db/db/" />  
                  <property name="bdbName" value="DB1" />  
                  <property name="logPath" value="${wayback.basedir}/file-db/db.log" />  
                </bean>  
              -->
    2. Comment out the `BDBCollection.xml` resource:

              <!--  
                <import resource="BDBCollection.xml"/>  
              -->
    3. Un-comment out the `RemoteCollection.xml` resource:

              <import resource="RemoteCollection.xml"/>
    4. Change the reference of the `collection` property from `localbdbcollection` to `remotecollection`:

              <property name="collection" ref="remotecollection" />
  
  * Edit the file `wayback-webapp/src/main/webapp/WEB-INF/RemoteCollection.xml`:
    1. Change the value of the `prefix` property from `http://wayback.archive-it.org/fileproxy/` to `http://reposervicehost:reposerviceport/warcs/`:

              <property name="prefix" value="http://reposervicehost:reposerviceport/warcs/" />
    2. Change the value of the `searchUrlBase` property from `http://wayback.archive-it.org/1055/xmlquery` to `http://reposervicehost:reposerviceport/cdx/owb/collectionid`:
              <property name="searchUrlBase" value="http://reposervicehost:reposerviceport/cdx/owb/collectionid" />

# Configuring PyWayback to use this service for indexing and replay
  Assuming that the LAAWS Repository service is running on host `reposervicehost` at port `reposerviceport` and it has a collection `collectionid`:
  
  * Edit the file `config.yaml`:
    1. After the definition of the `pywb` collection, add a new collection definition:

              collectionid:
                 archive_paths: http://reposervicehost:reposerviceport/warcs/
                 index:
                     type: cdx
                     api_url: http://reposervicehost:reposerviceport/cdx/pywb/collectionid?url={url}&matchType={matchType}&sort={sort}&closest={closest}&output={output}&fl={fl}
                     replay_url: ""
  