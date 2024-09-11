<!--

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

--> 
# LOCKSS Repository Service
This is the REST Web Service for the LOCKSS content repository.

## Note on branches
The `master` branch is for stable releases and the `develop` branch is for
ongoing development.

## Standard build and deployment
The LOCKSS cluster, including this project, is normally built and deployed using
the LOCKSS Installer, which uses `kubernetes`.

You can find more information about the installation of the LOCKSS system in the
[LOCKSS system manual](https://docs.lockss.org/projects/manual).

## Development build and deployment
### Clone the repo
`git clone -b develop ssh://github.com/lockss/laaws-repository-service.git`

### Create the Eclipse project (if so desired)
`File` -> `Import...` -> `Maven` -> `Existing Maven Projects`

### Build the web service:
In the home directory of this project, where this `README.md` file resides,
run `mvn clean install`.

This will run the tests as a pre-requisite for the build.

The result of the build is a so-called "uber JAR" file which includes the
project code plus all its dependencies and which can be located via the symbolic
link at

`./target/current-with-deps.jar`

### Run the web service:
Run the
[LOCKSS Development Scripts](https://github.com/lockss/laaws-dev-scripts)
project `bin/runservice` script in the home directory of this project, where
this `README.md` file resides.

The log is at `./logs/app.log`.

The API is documented at <http://127.0.0.1:24610/swagger-ui.html>.

The status of the web service may be obtained at
<http://127.0.0.1:24610/status>.

The administration UI of the web service is at <http://127.0.0.1:24611>.

### Configuring OpenWayback to use this service for indexing and replay
  Assuming that the LOCKSS Repository service is running on host
  `reposervicehost` at port `reposerviceport` and it has a collection
  `collectionid`:
  
  * Edit the OpenWayback file
    `wayback-webapp/src/main/webapp/WEB-INF/wayback.xml`:
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
    4. Change the reference of the `collection` property from
       `localbdbcollection` to `remotecollection`:

              <property name="collection" ref="remotecollection" />
  
  * Edit the OpenWayback file
    `wayback-webapp/src/main/webapp/WEB-INF/RemoteCollection.xml`:
    1. Change the value of the `prefix` property from
       `http://wayback.archive-it.org/fileproxy/` to
       `http://reposervicehost:reposerviceport/warcs/`:

              <property name="prefix" value="http://reposervicehost:reposerviceport/warcs/" />
    2. Change the value of the `searchUrlBase` property from
       `http://wayback.archive-it.org/1055/xmlquery` to
       `http://reposervicehost:reposerviceport/cdx/owb/collectionid`:

              <property name="searchUrlBase" value="http://reposervicehost:reposerviceport/cdx/owb/collectionid" />

### Configuring OpenWayback to use BASIC authentication
  Assuming that the authenticated user name is `lockss-u`, the password of this
  user is `lockss-p` and that the home directory of the Tomcat 8 web
  application server is `/usr/share/tomcat8`:
  
  * Edit the Tomcat file `/usr/share/tomcat8/conf/tomcat-users.xml`:
    1. Right before the `</tomcat-users>` at the end of the file. add:

              <role rolename="wayback"/>
              <user username="lockss-u" password="lockss-p" roles="wayback"/>
  
  * Edit the OpenWayback file
    `wayback-webapp/src/main/webapp/WEB-INF/web.xml`:
    1. Un-comment out the `<security-role>`, `<security-constraint>`,
       `<login-config>` and `<error-page>` elements at the end of the
       file.
    2. Change the value of the `url-pattern` property in the
       `web-resource-collection` property from `/usersecure/*` to
       `/wayback/*`:

              <url-pattern>/wayback/*</url-pattern>

### Configuring PyWayback to use this service for indexing and replay
  Assuming that the LAAWS Repository service is running on host
  `reposervicehost` at port `reposerviceport` and it has a collection
  `collectionid`:
  
  * Edit the file `config.yaml`:
    1. After the definition of the `pywb` collection, add a new
       collectiondefinition:

              collectionid:
                 archive_paths: http://reposervicehost:reposerviceport/warcs/
                 index:
                     type: cdx
                     api_url: http://reposervicehost:reposerviceport/cdx/pywb/collectionid?url={url}&matchType={matchType}&sort={sort}&closest={closest}&output={output}&fl={fl}
                     replay_url: ""
  