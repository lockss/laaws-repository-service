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
import org.lockss.laaws.rs.io.index.VolatileArtifactIndex;
import org.lockss.laaws.rs.io.index.LocalArtifactIndex;
import org.lockss.laaws.rs.io.index.solr.SolrArtifactIndex;
import org.lockss.log.L4JLogger;
import org.lockss.util.StringUtil;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.io.File;
import javax.annotation.Resource;

/**
 * Spring configuration beans for the configuration of the Repository Service's internal artifact index.
 */
@Configuration
public class ArtifactIndexConfig {
  private final static L4JLogger log = L4JLogger.getLogger();

  public final static String INDEX_SPEC_KEY = "repo.index.spec";

  public final static String LOCAL_PERSISTINDEXNAME_KEY = "repo.persistIndexName";

  public final static String SOLR_BASEURL_KEY = "repo.index.solr.solrUrl";
  public final static String SOLR_COLLECTION_KEY = "repo.index.solr.solrCollection";

  @Resource
  private Environment env;

  @Bean
  public ArtifactIndex setArtifactIndex() {
    // Get repo and index spec from prop
    String repoSpec = env.getProperty(LockssRepositoryConfig.REPO_SPEC_KEY);
    String indexSpec = env.getProperty(INDEX_SPEC_KEY);

    return createArtifactIndex(parseIndexSpecs(repoSpec, indexSpec));
  }

  private String parseIndexSpecs(String repoSpec, String indexSpec) {
    if (StringUtil.isNullString(repoSpec)) {
      log.error("Missing repository configuration");
      throw new IllegalStateException("Repository not configured");
    }

    // Parse repo spec for repo type
    String[] repoSpecParts = repoSpec.split(":", 2);
    String repoType = repoSpecParts[0].trim().toLowerCase();

    switch (repoType) {
      case "volatile":
        // Disable creation of ArtifactIndex bean; allow LockssRepositoryConfig to
        // create a VolatileLockssRepository
        return null;

      case "local":
        // Support for legacy repo.spec=local:X;Y;Z
        return "local";

      case "custom":
        // Support for repo.spec=custom and repo.index.spec=X
        if (StringUtil.isNullString(indexSpec)) {
          log.error("Missing artifact index configuration");
          throw new IllegalStateException("Artifact index not configured");
        }

        return indexSpec.trim().toLowerCase();

      default:
        throw new IllegalArgumentException("Repository spec not supported: " + repoSpec);
    }
  }

  private ArtifactIndex createArtifactIndex(String indexType) {
    log.trace("indexType = {}", indexType);

    if (StringUtil.isNullString(indexType)) {
      return null;
    }

    switch (indexType) {
      case "volatile":
        return new VolatileArtifactIndex();

      case "local":
        // Get name of locally persisted index
        String repositoryPersistIndexName = env.getProperty(LOCAL_PERSISTINDEXNAME_KEY);
        log.trace("repositoryPersistIndexName = {}", repositoryPersistIndexName);

        // Get base directory paths for the local index
        String baseDirsProp = env.getProperty(ArtifactDataStoreConfig.LOCAL_BASEDIRS_KEY);

        if (baseDirsProp == null) {
          // Fallback to previous property key
          baseDirsProp = env.getProperty(ArtifactDataStoreConfig.LOCAL_BASEDIRS_FALLBACK_KEY);

          if (baseDirsProp == null) {
            log.error("No local base directories specified");
            throw new IllegalArgumentException("No local base dirs");
          }
        }

        String[] baseDirs = baseDirsProp.split(";");

        // Create a local artifact index persisting to the first local data store directory
        return new LocalArtifactIndex(new File(baseDirs[0]), repositoryPersistIndexName);

      case "solr":
        String solrCollection = env.getProperty(SOLR_COLLECTION_KEY);
        String solrBaseUrl = env.getProperty(SOLR_BASEURL_KEY);

        if (StringUtil.isNullString(solrBaseUrl)) {
          log.error("Missing Solr base URL endpoint");
          throw new IllegalArgumentException("Missing Solr base URL endpoint");
        }

        if (!StringUtil.isNullString(solrCollection)) {
          return new SolrArtifactIndex(solrBaseUrl, solrCollection);
        }

        return new SolrArtifactIndex(solrBaseUrl);

      default:
        String errMsg = String.format("Unknown artifact index '%s'", indexType);
        log.error(errMsg);
        throw new IllegalArgumentException(errMsg);
    }
  }
}
