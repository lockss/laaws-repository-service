# `laaws-repository-service` Release Notes

## 2.15.0
## changes since 2.12.0
* Remove travis CI support
* Upgrade to Spring 6.x and Spring Boot 3.x related changes
* Upgrade to Surefire 5.2.2, JUnit 5.9.3 (Surefire's version).
* Use jakarta components
* Update Spring servlet parameters

### Refactorings

* Added new constructor which takes a base path for temp files
* Refactored multipart handling to use configuration parameters; 
* delete unnecessary WRITE_DATES_AS_TIMESTAMPS
* Refactored LockssApplicationPart to have a getDigest()
* Better documentation, added javadoc
* Clean up unused classes
* suppress extraneous messages in tests

* Configured SoapApplication with a StateManager
* HttpStatus / HttpStatusCode refactor
* Reverted removal of DigestFileItemFactory and DigestFileItem;
* modified ArtifactsApiServiceImpl to call LockssMultipartFile#getDigest().
* Introduced and use custom multipart resolver to modify MultipartFile#getContentType() behavior
* Configured RepositoryApplication with a StateManager
* Replace Thread with LockssThread
* Added support for handling large response with RestLockssRepository

* WIP: getPart(String name) implementation
* WIP: Proof-of-concept custom parsing of multipart requests
* WIP: Fix timestamp type
* WIP: Refactored to use Spring's new multipart API
* WIP: Removed SolrAutoConfiguration

### API Changes
* cleanup  swagger file ordering.
* cleanup swagger to remove allof in Artifact list only expected fields
* cleanup swagger comments and remove unused application properties
* make "op" in  /aus/{auid}/bulk a query arg

* Modified codegen post-processing to fix imports in new classes
* Remove generated SwaggerDocumentConfig
* Moved REST endpoints needed for SOAP to under /ws

### Bug Fixes
* Disable ServletWebServerApplicationContext kludge
* Remove redundant errorAttributes bean from RepositoryServiceSpringConfig
* Remove circular Spring bean reference in RepositoryServiceSpringConfig
* Resolve issue #332: Omit stacktrace when logging MalformedStreamException
* Fixed: Do not make all parts required 
* Fixed: Parsing of "committed" parameter from PUT requests
* Fixed: Send empty multipart in PUT request
* Use correct name of MultipartResolver bean


## 2.12.0

### Features
* The Repository Service now supports instance-local state. The state directory it uses can be adjusted by setting the
  `repo.state.dir` application property. The default is `/data/state`.
* When using a Solr artifact index, the interval (in milliseconds) between Solr hard commits can now be adjusted by 
  setting the `repo.index.solr.hardCommitInterval` application property. The default is `15000` (15 seconds).

## Changes Since 2.0.10.1

*   Switched to a 3-part version numbering scheme.

## 2.0.10.1

### Security

*   Out of an abundance of caution, re-released 2.0.10.0 with Jackson-Databind 2.9.10.8 (CVE-2021-20190).

## 2.0.10.0

### Features

*   ...

### Fixes

*   ...

## 2.0.9.0

### Features

*   REST services authenticate, clients provide credentials.
*   Improved startup coordination and ready waiting of all services and databases.
*   Improved coordination of initial plugin registry crawls.
*   `Artifact` and `ArtifactData` caching improves performance.
*   Paginating iterators improve performance.

### Fixes

*   Fix incorrect URL enumeration order.
*   Remove file size limit.
*   Increase URL length limit.
