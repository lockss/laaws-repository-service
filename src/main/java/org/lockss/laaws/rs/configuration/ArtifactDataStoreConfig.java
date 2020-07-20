/*
 * Copyright (c) 2019, Board of Trustees of Leland Stanford Jr. University,
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
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.io.storage.hdfs.HdfsWarcArtifactDataStore;
import org.lockss.laaws.rs.io.storage.local.LocalWarcArtifactDataStore;
import org.lockss.laaws.rs.io.storage.local.TestingWarcArtifactDataStore;
import org.lockss.laaws.rs.io.storage.warc.VolatileWarcArtifactDataStore;
import org.lockss.log.L4JLogger;
import org.lockss.app.LockssApp;
import org.lockss.util.PatternIntMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.core.env.Environment;
import org.springframework.data.hadoop.config.annotation.builders.HadoopConfigBuilder;

import javax.annotation.Resource;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Spring configuration beans for the configuration of the Repository Service's internal artifact data store.
 */
@Configuration
public class ArtifactDataStoreConfig {
  public final static String PARAM_FREE_SPACE_MAP =
    "org.lockss.repo.testing.freeSpaceMap";

  private final static L4JLogger log = L4JLogger.getLogger();

  private final static String DATASTORE_SPEC_KEY = "repo.datastore.spec";
  private final static String HDFS_SERVER_KEY = "repo.datastore.hdfs.server";
  private final static String HDFS_BASEDIR_KEY = "repo.datastore.hdfs.basedir";

  public final static String LOCAL_BASEDIRS_KEY = "repo.datastore.local.basedirs";
  public final static String LOCAL_BASEDIRS_FALLBACK_KEY = "repo.datastore.local.basedir";

  @Resource
  private Environment env;

  @Autowired
  ArtifactIndex index;

  TestingWarcArtifactDataStore twads;


  @Bean
  public ArtifactDataStore setArtifactStore() throws Exception {
    String repoSpec = env.getProperty(LockssRepositoryConfig.REPO_SPEC_KEY);
    String datastoreSpec = env.getProperty(DATASTORE_SPEC_KEY);

    if (!repoSpec.equals("custom")) {
      log.warn("Ignoring data store specification because a predefined repository specification is being used");
      return null;
    }

    if (datastoreSpec != null) {
      String dsType = datastoreSpec.trim().toLowerCase();
      switch (dsType) {
        case "hdfs":
          String hdfsServer = env.getProperty(HDFS_SERVER_KEY);
          Path hdfsBaseDir = Paths.get(env.getProperty(HDFS_BASEDIR_KEY));

          log.info(String.format(
              "Configuring HDFS artifact data store [%s, %s]",
              hdfsServer,
              hdfsBaseDir
          ));

          HadoopConfigBuilder config = new HadoopConfigBuilder();
          config.fileSystemUri(hdfsServer);

          return new HdfsWarcArtifactDataStore(index, config.build(), hdfsBaseDir);

        case "local":
        case "testing":
          String baseDirsProp = env.getProperty(LOCAL_BASEDIRS_KEY);

          if (baseDirsProp == null) {
            baseDirsProp = env.getProperty(LOCAL_BASEDIRS_FALLBACK_KEY);
            if (baseDirsProp == null) {
              log.error("No local base directories specified");
              throw new IllegalArgumentException("No local base dirs");
            }
          }

          String[] dirs = baseDirsProp.split(";");
          File[] baseDirs = Arrays.stream(dirs).map(File::new).toArray(File[]::new);
	  switch (dsType) {
	  case "local":
	    log.info("Configuring local artifact data store [baseDirs: {}]",
		     Arrays.asList(baseDirs));
	    return new LocalWarcArtifactDataStore(index, baseDirs);
	  case "testing":
	    log.info("Configuring testing artifact data store [baseDirs: {}]",
		     Arrays.asList(baseDirs));
	    twads = new TestingWarcArtifactDataStore(index, baseDirs);
	    return twads;
	  default:
	    throw new RuntimeException("Shouldn't happen");
	  }
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

  // Register config callback for TestingWarcArtifactDataStore after
  // LockssDaemon is started
  @EventListener(ApplicationReadyEvent.class)
  public void registerConfigCallback() {
    if (twads != null) {
      LockssApp.getLockssApp().getConfigManager().registerConfigurationCallback(new TestingArtifactDataStoreConfigCallback(twads));
    }
  }

  /** Configuration callback to set free space map for testing.
      Configuration mechanism isn't visible to repocore so config callback
      must be here. */
  private static class TestingArtifactDataStoreConfigCallback
    implements org.lockss.config.Configuration.Callback {
    TestingWarcArtifactDataStore twads;

    TestingArtifactDataStoreConfigCallback(TestingWarcArtifactDataStore ds) {
      twads = ds;
    }

    public void configurationChanged(org.lockss.config.Configuration newConfig,
				     org.lockss.config.Configuration oldConfig,
				     org.lockss.config.Configuration.Differences changedKeys) {
      PatternIntMap freeSpacePatternMap = PatternIntMap.EMPTY;
      List lst = newConfig.getList(PARAM_FREE_SPACE_MAP, null);
      if (lst != null && !lst.isEmpty()) {
	try {
	  freeSpacePatternMap = new PatternIntMap(lst);
	} catch (IllegalArgumentException e) {
	  log.error("Illegal testing disk space map, ignoring", e);
	}
      }
      twads.setTestingDiskSpaceMap(freeSpacePatternMap);
    }
  }
}
