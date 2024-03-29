# `laaws-repository-service` Release Notes

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
