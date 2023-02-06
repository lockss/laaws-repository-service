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

import org.lockss.app.LockssApp;
import org.lockss.config.ConfigManager;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.DispatchingArtifactIndex;
import org.lockss.laaws.rs.io.index.LocalArtifactIndex;
import org.lockss.laaws.rs.io.index.VolatileArtifactIndex;
import org.lockss.laaws.rs.io.index.solr.SolrArtifactIndex;
import org.lockss.log.L4JLogger;
import org.lockss.util.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import java.io.IOException;
import java.util.List;

/**
 * Spring configuration beans for the configuration of the Repository Service's internal artifact index.
 */
@Configuration
public class ArtifactIndexConfig {
  /**
   * Determines how frequently Solr hard commits are performed. (And
   * checks for Solr restart lossage.).
   */
  public final static String PARAM_SOLR_HARDCOMMTI_INTERVAL =
    "org.lockss.repo.index.solr.hardCommitInterval";
  public final static long DEFAULT_SOLR_HARDCOMMTI_INTERVAL =
    SolrArtifactIndex.DEFAULT_SOLR_HARDCOMMIT_INTERVAL;

  private final static L4JLogger log = L4JLogger.getLogger();

  private RepositoryServiceProperties repoProps;
  private SolrArtifactIndex solrIndex;

  @Autowired
  public ArtifactIndexConfig(RepositoryServiceProperties repoProps) {
    this.repoProps = repoProps;
  }

  @Bean
  public ArtifactIndex setArtifactIndex() {
    return createArtifactIndex(parseIndexSpecs());
  }

  @Autowired
  private ApplicationArguments appArgs;

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
        // Get Solr BasicAuth credentials from LockssApp
        List<String> credentials = getSolrCredentials();

        if (!StringUtil.isNullString(repoProps.getSolrCollectionName())) {
          // Use provided Solr collection name
          solrIndex = new SolrArtifactIndex(repoProps.getSolrEndpoint(), repoProps.getSolrCollectionName(), credentials)
              .setHardCommitInterval(repoProps.getSolrHardCommitInterval());
          return solrIndex;
        }

        solrIndex = new SolrArtifactIndex(repoProps.getSolrEndpoint(), credentials)
            .setHardCommitInterval(repoProps.getSolrHardCommitInterval());
        return solrIndex;

      case "dispatching":
        // Create Solr index
        ArtifactIndex solrIndex = createArtifactIndex("solr");

        // Create Dispatching with Solr
        return new DispatchingArtifactIndex(solrIndex);

      default:
        String errMsg = String.format("Unknown artifact index: '%s'", indexType);
        log.error(errMsg);
        throw new IllegalArgumentException(errMsg);
    }
  }

  /**
   * Read the SOLR client credentials from the file specified on the command line.
   *
   * @return A list of username and password, or null if none specified or
   * file doesn't exist.
   */
  private List<String> getSolrCredentials() {
    log.debug("getNonOptionArgs: {}", appArgs.getNonOptionArgs());

    LockssApp.StartupOptions startOpts =
        LockssApp.getStartupOptions(appArgs.getNonOptionArgs());

    String filename = startOpts.getSecretFileFor("solr");

    if (filename != null) {
      try {
        LockssApp.ClientCredentials cred = LockssApp.readClientCredentials(filename);
        return cred.getCredentialsAsList();
      } catch (IOException e) {
        log.warn("Couldn't read SOLR credentials from file {}", filename, e);
      }
    }

    return null;
  }

  // Register config callback once ConfigManager has been created.
  @EventListener
  public void configMgrCreated(ConfigManager.ConfigManagerCreatedEvent event) {
    log.debug2("ConfigManagerCreatedEvent triggered");
    ConfigManager.getConfigManager()
        .registerConfigurationCallback(new ArtifactIndexConfigCallback());
  }

  private class ArtifactIndexConfigCallback
    implements org.lockss.config.Configuration.Callback {

    public void configurationChanged(org.lockss.config.Configuration newConfig,
                                     org.lockss.config.Configuration oldConfig,
                                     org.lockss.config.Configuration.Differences changedKeys) {

      if (changedKeys.contains(PARAM_SOLR_HARDCOMMTI_INTERVAL)) {
        if (solrIndex != null) {
          solrIndex.setHardCommitInterval(newConfig.getTimeInterval(PARAM_SOLR_HARDCOMMTI_INTERVAL,
                                                                    DEFAULT_SOLR_HARDCOMMTI_INTERVAL));
        }
      }
    }
  }
}
