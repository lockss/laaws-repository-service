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
import org.lockss.app.LockssApp;
import org.lockss.log.L4JLogger;
import org.lockss.util.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.io.File;
import java.io.IOException;
import java.util.List;
import javax.annotation.Resource;

/**
 * Spring configuration beans for the configuration of the Repository Service's internal artifact index.
 */
@Configuration
public class ArtifactIndexConfig {
    private final static L4JLogger log =  L4JLogger.getLogger();
    private final static String INDEX_SPEC_KEY = "repo.index.spec";

    private final static String SOLR_BASEURL_KEY = "repo.index.solr.solrUrl";
    private final static String SOLR_COLLECTION_KEY = "repo.index.solr.solrCollection";

  @Resource
    private Environment env;

    @Autowired
      private ApplicationArguments  appArgs;

    @Bean
    public ArtifactIndex setArtifactIndex() {
        String repoSpec = env.getProperty(LockssRepositoryConfig.REPO_SPEC_KEY);
        String indexSpec = env.getProperty(INDEX_SPEC_KEY);

        if (!repoSpec.equals("custom")) {
            log.warn("Ignoring index specification because a predefined repository specification is being used");
            return null;
        }

        log.info(String.format("indexSpec = %s", indexSpec));

        if (indexSpec != null) {
            switch (indexSpec.trim().toLowerCase()) {
                case "solr":
                    String solrCollection = env.getProperty(SOLR_COLLECTION_KEY);
                    String solrBaseUrl = env.getProperty(SOLR_BASEURL_KEY);

                    // Read the SOLR client credentials.  Note: this may
                    // delete the secrets file.  If the SOLR credentials
                    // are requested by some part of the daemon, using the
                    // normal facility
                    // (LockssApp.getReadClientCredentials()) (which can't
                    // be called here because LockssApp hasn't started
                    // yet), it will fail because the secrets file has
                    // already been deleted.

                    List<String> solrCred = getSolrCredentials();
                    log.info("Solr cred: {}", solrCred);

                    if (solrCollection != null || !solrCollection.isEmpty()) {
                      return new SolrArtifactIndex(solrBaseUrl, solrCollection);
                    }

                    return new SolrArtifactIndex(solrBaseUrl);

                case "volatile":
                    return new VolatileArtifactIndex();

                case "local":
		  String baseDirsProp = env.getProperty(ArtifactDataStoreConfig.LOCAL_BASEDIRS_KEY);
		  if (baseDirsProp == null) {
		    baseDirsProp = env.getProperty(ArtifactDataStoreConfig.LOCAL_BASEDIRS_FALLBACK_KEY);
		    if (baseDirsProp == null) {
		      log.error("No local base directories specified");
		      throw new IllegalArgumentException("No local base dirs");
		    }
		  }
		  String[] baseDirs = baseDirsProp.split(";");
		  return new LocalArtifactIndex(new File(baseDirs[0]),
						env.getProperty(LockssRepositoryConfig.REPO_PERSISTINDEXNAME_KEY));

                default:
                    String errMsg = String.format("Unknown index specification '%s'", indexSpec);
                    log.error(errMsg);
                    throw new IllegalArgumentException(errMsg);
            }
        }

        log.warn("No artifact index specification set; setting ArtifactIndex bean to null");
        return null;
    }

  /** Read the SOLR client credentials from the file specified on the
   * command line.
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
        LockssApp.ClientCredentials cred =
          LockssApp.readClientCredentials(filename);
        return cred.getCredentialsAsList();
      } catch (IOException e) {
        log.warn("Couldn't read SOLR credentials from file {}", filename, e);
      }
    }
    return null;
  }
}
