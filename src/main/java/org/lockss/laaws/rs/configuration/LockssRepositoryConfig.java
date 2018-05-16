/*
 * Copyright (c) 2017-2018, Board of Trustees of Leland Stanford Jr. University,
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
import org.lockss.laaws.rs.core.*;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import javax.annotation.Resource;
import java.io.File;
import java.net.URL;

/**
 * Spring configuration beans for the configuration of the Repository Service's internal repository.
 */
@Configuration
public class LockssRepositoryConfig {
    private final static Log log = LogFactory.getLog(LockssRepositoryConfig.class);
    private final static String REPO_SPEC_KEY = "repo.spec";
    private final static String REPO_PERSISTINDEXNAME_KEY = "repo.persistIndexName";

    @Autowired
    ArtifactDataStore store;

    @Autowired
    ArtifactIndex index;

    @Resource
    private Environment env;

    @Bean
    public LockssRepository createRepository() throws Exception {
        String repositorySpecification = env.getProperty(REPO_SPEC_KEY);
        String repositoryPersistIndexName = env.getProperty(REPO_PERSISTINDEXNAME_KEY);
        if (log.isDebugEnabled()) {
            log.debug(String.format(
                    "Starting internal LOCKSS repository (repositorySpecification = %s)",
                    repositorySpecification
            ));
            log.debug(String.format("repositoryPersistIndexName = %s)",
                repositoryPersistIndexName
        ));
        }

        if (repositorySpecification != null) {
            switch (repositorySpecification.trim().toLowerCase()) {
                case "volatile":
                    return LockssRepositoryFactory.createVolatileRepository();

                case "custom":
                    // Configure artifact index and data store individually using Spring beans (see their
                    // configuration beans in)the ArtifactIndexConfig and ArtifactDataStoreConfig classes,
                    // respectively.
                    return new BaseLockssRepository(index, store);

                default: {
                    if (!(repositorySpecification.indexOf(':') < 0)) {
                        String[] specParts = repositorySpecification.split(":", 2);

                        // Get the type of implementation configured.
                        String repositoryType = specParts[0].trim().toLowerCase();

                        if (log.isDebugEnabled())
                            log.debug("repositoryType = " + repositoryType);

                        // Check whether a local implementation is configured.
                        switch (repositoryType) {
                            case "local": {
                                // Yes: Get the configured filesystem path.
                                String repositoryLocalPath = specParts[1];

                                if (log.isDebugEnabled())
                                    log.debug("repositoryLocalPath = " + repositoryLocalPath);

                                return new LocalLockssRepository(new File(repositoryLocalPath), repositoryPersistIndexName);
                            }

                            case "rest": {
                                String repositoryRestUrl = specParts[1];

                                if (log.isDebugEnabled())
                                    log.debug("repositoryRestUrl = " + repositoryRestUrl);

                                return new RestLockssRepository(new URL(repositoryRestUrl));
                            }

                            default: {
                                String errMsg = String.format(
                                        "Unknown repository type '%s'; cannot continue",
                                        repositoryType
                                );
                                log.error(errMsg);
                                throw new IllegalArgumentException(errMsg);
                            }
                        }
                    }

                    // Unknown repository specification
                    String errMsg = String.format(
                            "Unknown repository specification '%s'; cannot continue",
                            repositorySpecification
                    );
                    log.error(errMsg);
                    throw new IllegalArgumentException(errMsg);
                }
            }
        }

        log.warn("No internal LOCKSS repository specified; using volatile implementation");
        return LockssRepositoryFactory.createVolatileRepository();
    }
}
