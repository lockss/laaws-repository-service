#!/bin/bash
#
# Copyright (c) 2018-2020 Board of Trustees of Leland Stanford Jr. University,
# all rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
# STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
# IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# Except as contained in this notice, the name of Stanford University shall not
# be used in advertising or otherwise to promote the sale, use or other dealings
# in this Software without prior written authorization from Stanford University.
#

# Fixes the code generated by Swagger Codegen.

function fixImport() {
JAVA_SRC=$1
sed -i.backup "s/import $2/import $3/" $JAVA_SRC && rm $JAVA_SRC.backup
}

# Edit StatusApi.java.
STATUS_API=src/generated/java/org/lockss/laaws/rs/api/StatusApi.java
fixImport $STATUS_API org.lockss.laaws.rs.model.ApiStatus org.lockss.util.rest.status.ApiStatus

# Edit StatusApiDelegate.java.
STATUS_API_DELEGATE=src/generated/java/org/lockss/laaws/rs/api/StatusApiDelegate.java
fixImport $STATUS_API_DELEGATE org.lockss.laaws.rs.model.ApiStatus org.lockss.util.rest.status.ApiStatus

# Edit ArchivesApi.java.
ARCHIVES_API=src/generated/java/org/lockss/laaws/rs/api/ArchivesApi.java
fixImport $ARCHIVES_API org.lockss.laaws.rs.model.StreamingResponseBody org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody

# Edit ArchivesApiDelegate.java.
ARCHIVES_API_DELEGATE=src/generated/java/org/lockss/laaws/rs/api/ArchivesApiDelegate.java
fixImport $ARCHIVES_API_DELEGATE org.lockss.laaws.rs.model.StreamingResponseBody org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody

# Edit ArtifactsApi.java.
ARTIFACTS_API=src/generated/java/org/lockss/laaws/rs/api/ArtifactsApi.java
fixImport $ARTIFACTS_API org.lockss.laaws.rs.model.Artifact org.lockss.util.rest.repo.model.Artifact
fixImport $ARTIFACTS_API org.lockss.laaws.rs.model.ArtifactPageInfo org.lockss.util.rest.repo.model.ArtifactPageInfo

# Edit ArtifactsApiDelegate.java.
ARTIFACTS_API_DELEGATE=src/generated/java/org/lockss/laaws/rs/api/ArtifactsApiDelegate.java
fixImport $ARTIFACTS_API_DELEGATE org.lockss.laaws.rs.model.Artifact org.lockss.util.rest.repo.model.Artifact
fixImport $ARTIFACTS_API_DELEGATE org.lockss.laaws.rs.model.ArtifactPageInfo org.lockss.util.rest.repo.model.ArtifactPageInfo

# Edit AusApi.java.
AUS_API=src/generated/java/org/lockss/laaws/rs/api/AusApi.java
fixImport $AUS_API org.lockss.laaws.rs.model.ArtifactPageInfo org.lockss.util.rest.repo.model.ArtifactPageInfo
fixImport $AUS_API org.lockss.laaws.rs.model.AuidPageInfo org.lockss.util.rest.repo.model.AuidPageInfo
fixImport $AUS_API org.lockss.laaws.rs.model.AuSize org.lockss.util.rest.repo.model.AuSize

# Edit AusApiDelegate.java.
AUS_API_DELEGATE=src/generated/java/org/lockss/laaws/rs/api/AusApiDelegate.java
fixImport $AUS_API_DELEGATE org.lockss.laaws.rs.model.ArtifactPageInfo org.lockss.util.rest.repo.model.ArtifactPageInfo
fixImport $AUS_API_DELEGATE org.lockss.laaws.rs.model.AuidPageInfo org.lockss.util.rest.repo.model.AuidPageInfo
fixImport $AUS_API_DELEGATE org.lockss.laaws.rs.model.AuSize org.lockss.util.rest.repo.model.AuSize

# Edit RepoinfoApi.java.
REPOINFO_API=src/generated/java/org/lockss/laaws/rs/api/RepoinfoApi.java
fixImport $REPOINFO_API org.lockss.laaws.rs.model.RepositoryInfo org.lockss.util.rest.repo.model.RepositoryInfo

# Edit RepoinfoApiDelegate.java.
REPOINFO_API_DELEGATE=src/generated/java/org/lockss/laaws/rs/api/RepoinfoApiDelegate.java
fixImport $REPOINFO_API_DELEGATE org.lockss.laaws.rs.model.RepositoryInfo org.lockss.util.rest.repo.model.RepositoryInfo
