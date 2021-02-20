/*
 * Copyright (c) 2017-2019, Board of Trustees of Leland Stanford Jr. University,
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.lockss.laaws.rs.configuration;

import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.LocalArtifactIndex;
import org.lockss.laaws.rs.io.index.VolatileArtifactIndex;
import org.lockss.laaws.rs.io.index.solr.SolrArtifactIndex;
import org.lockss.log.L4JLogger;
import org.lockss.util.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration beans for the configuration of the Repository Service's internal artifact index.
 */
@Configuration
public class ArtifactIndexConfig {
  private final static L4JLogger log = L4JLogger.getLogger();

  private RepositoryServiceProperties repoProps;

  @Autowired
  public ArtifactIndexConfig(RepositoryServiceProperties repoProps) {
    this.repoProps = repoProps;
  }

  @Bean
  public ArtifactIndex setArtifactIndex() {
    return createArtifactIndex(parseIndexSpecs());
  }

  private String parseIndexSpecs() {
    switch (repoProps.getRepositoryType()) {
      case "volatile":
        // Allow a volatile index to be created so that WARC compression can be configured
        // in the volatile artifact data store
        return "volatile";

      case "local":
        // Support for legacy repo.spec=local:X;Y;Z
        return "local";

      case "custom":
        return repoProps.getIndexSpec();

      default:
        throw new IllegalArgumentException("Repository spec not supported: " + repoProps.getRepositorySpec());
    }
  }

  private ArtifactIndex createArtifactIndex(String indexType) {
    log.trace("indexType = {}", indexType);

    switch (indexType) {
      case "volatile":
        return new VolatileArtifactIndex();

      case "local":
        // Create a local artifact index *persisting to the first local data store directory*!
        return new LocalArtifactIndex(repoProps.getLocalBaseDirs()[0], repoProps.getLocalPersistIndexName());

      case "solr":
        if (!StringUtil.isNullString(repoProps.getSolrCollectionName())) {
          // Use provided Solr collection name
          return new SolrArtifactIndex(repoProps.getSolrEndpoint(), repoProps.getSolrCollectionName());
        }

        return new SolrArtifactIndex(repoProps.getSolrEndpoint());

      default:
        String errMsg = String.format("Unknown artifact index: '%s'", indexType);
        log.error(errMsg);
        throw new IllegalArgumentException(errMsg);
    }
  }
}
