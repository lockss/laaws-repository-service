/*
 * Copyright (c) 2017, Board of Trustees of Leland Stanford Jr. University,
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
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.io.storage.hdfs.HdfsWarcArtifactDataStore;
import org.lockss.laaws.rs.io.storage.local.LocalWarcArtifactDataStore;
import org.lockss.laaws.rs.io.storage.warc.VolatileWarcArtifactDataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.hadoop.config.annotation.builders.HadoopConfigBuilder;

import javax.annotation.Resource;
import java.io.File;

/**
 * Spring configuration beans for the configuration of the Repository Service's internal artifact data store.
 */
@Configuration
public class ArtifactDataStoreConfig {
    private final static Log log = LogFactory.getLog(ArtifactDataStoreConfig.class);

    private final static String DATASTORE_SPEC_KEY = "repo.datastore.spec";
    private final static String HDFS_SERVER_KEY = "repo.datastore.hdfs.server";
    private final static String HDFS_BASEDIR_KEY = "repo.datastore.hdfs.basedir";
    private final static String LOCAL_BASEDIR_KEY = "repo.datastore.local.basedir";

    @Resource
    private Environment env;

    @Autowired
    ArtifactIndex index;

    @Bean
    public ArtifactDataStore setArtifactStore() throws Exception {
        String repoSpec = env.getProperty(LockssRepositoryConfig.REPO_SPEC_KEY);
        String datastoreSpec = env.getProperty(DATASTORE_SPEC_KEY);

        if (!repoSpec.equals("custom")) {
            log.warn("Ignoring data store specification because a predefined repository specification is being used");
            return null;
        }

        if (datastoreSpec != null) {
            switch (datastoreSpec.trim().toLowerCase()) {
                case "hdfs":
                    String hdfsServer = env.getProperty(HDFS_SERVER_KEY);
                    String hdfsBaseDir = env.getProperty(HDFS_BASEDIR_KEY);

                    log.info(String.format(
                            "Configuring HDFS artifact data store [%s, %s]",
                            hdfsServer,
                            hdfsBaseDir
                    ));

                    HadoopConfigBuilder config = new HadoopConfigBuilder();
                    config.fileSystemUri(hdfsServer);
                    return new HdfsWarcArtifactDataStore(index, config.build(), hdfsBaseDir);

                case "local":
                    String baseDir = env.getProperty(LOCAL_BASEDIR_KEY);
                    log.info(String.format("Configuring local filesystem artifact data store [%s]", baseDir));
                    return new LocalWarcArtifactDataStore(index, new File(baseDir));

                case "volatile":
                    log.info("Configuring volatile artifact data store");
                    return new VolatileWarcArtifactDataStore(index);

                default:
                    String errMsg = String.format("Unknown data store specification '%s'", datastoreSpec);
                    log.error(errMsg);
                    throw new IllegalArgumentException(errMsg);
            }
        }

        log.warn("No artifact store specification set; setting ArtifactDataStore bean to null");
        return null;
    }
}
