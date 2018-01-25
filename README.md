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