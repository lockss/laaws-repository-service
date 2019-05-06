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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.VolatileArtifactIndex;
import org.lockss.laaws.rs.io.index.solr.SolrArtifactIndex;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import javax.annotation.Resource;

/**
 * Spring configuration beans for the configuration of the Repository Service's internal artifact index.
 */
@Configuration
public class ArtifactIndexConfig {
    private final static Log log = LogFactory.getLog(ArtifactIndexConfig.class);
    private final static String INDEX_SPEC_KEY = "repo.index.spec";
    private final static String SOLR_URL_KEY = "repo.index.solr.solrUrl";

    // Application properties keys for authentication credentials.
    private final static String SOLR_USER_KEY = "repo.index.solr.user";
    private final static String SOLR_PASSWORD_KEY = "repo.index.solr.password";

    @Resource
    private Environment env;

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
                    return new SolrArtifactIndex(env.getProperty(SOLR_URL_KEY),
                	env.getProperty(SOLR_USER_KEY),
                	env.getProperty(SOLR_PASSWORD_KEY));

                case "volatile":
                    return new VolatileArtifactIndex();

                default:
                    String errMsg = String.format("Unknown index specification '%s'", indexSpec);
                    log.error(errMsg);
                    throw new IllegalArgumentException(errMsg);
            }
        }

        log.warn("No artifact index specification set; setting ArtifactIndex bean to null");
        return null;
    }
}
