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
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.log.L4JLogger;
import org.lockss.app.LockssApp;
import org.lockss.util.PatternIntMap;
import org.lockss.util.StringUtil;
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
  private final static L4JLogger log = L4JLogger.getLogger();

  public final static String PARAM_FREE_SPACE_MAP =
      "org.lockss.repo.testing.freeSpaceMap";

  /**
   * Enables or disables the use of GZIP compression for WARC files in
   * WARC artifact data store implementations.
   */
  public final static String PARAM_REPO_USE_WARC_COMPRESSION =
      "org.lockss.repo.warc.useCompression";

  /**
   * Default settings for use of GZIP compression for WARC files.
   */
  public final static boolean DEFAULT_REPO_USE_WARC_COMPRESSION = false;

  public final static String DATASTORE_USE_WARC_COMPRESSION_KEY = "repo.datastore.warc.useCompression";

  public final static String DATASTORE_SPEC_KEY = "repo.datastore.spec";

  public final static String HDFS_SERVER_KEY = "repo.datastore.hdfs.server";
  public final static String HDFS_BASEDIR_KEY = "repo.datastore.hdfs.basedir";

  public final static String LOCAL_BASEDIRS_KEY = "repo.datastore.local.basedirs";
  public final static String LOCAL_BASEDIRS_FALLBACK_KEY = "repo.datastore.local.basedir";

  @Resource
  private Environment env;

  @Autowired
  ArtifactIndex index;

  WarcArtifactDataStore ds;

  @Bean
  public ArtifactDataStore createArtifactDataStore() throws Exception {
    // Get the repo and data store spec from Spring
    String repoSpec = env.getProperty(LockssRepositoryConfig.REPO_SPEC_KEY);
    String datastoreSpec = env.getProperty(DATASTORE_SPEC_KEY);

    // Get and parse useWarcCompression property
    String useWarcCompressionProp = env.getProperty(DATASTORE_USE_WARC_COMPRESSION_KEY);
    boolean useWarcCompression = StringUtil.isNullString(useWarcCompressionProp) ?
        DEFAULT_REPO_USE_WARC_COMPRESSION : Boolean.parseBoolean(useWarcCompressionProp);

    // Create WARC artifact data store and set use WARC compression
    ds = createWarcArtifactDataStore(parseDataStoreSpecs(repoSpec, datastoreSpec));

    if (ds != null) {
      ds.setUseWarcCompression(useWarcCompression);
    }

    // Return data store
    return ds;
  }

  private String parseDataStoreSpecs(String repoSpec, String datastoreSpec) {
    if (StringUtil.isNullString(repoSpec)) {
      log.error("Missing repository configuration");
      throw new IllegalStateException("Repository not configured");
    }

    // Parse repo spec for repo type
    String[] repoSpecParts = repoSpec.split(":", 2);
    String repoType = repoSpecParts[0].trim().toLowerCase();

    switch (repoType) {
      case "volatile":
        // Disable creation of ArtifactDataStore bean; allow LockssRepositoryConfig to
        // create a VolatileLockssRepository
        return null;

      case "local":
        // Support for legacy repo.spec=local:X;Y;Z parameter
        return "local";

      case "custom":
        // Support for repo.spec=custom and repo.datastore.spec=X
        if (StringUtil.isNullString(datastoreSpec)) {
          log.error("Missing artifact data store configuration");
          throw new IllegalStateException("Artifact data store not configured");
        }

        // Parse the data store type from data store spec
        return datastoreSpec.trim().toLowerCase();

      default:
        throw new IllegalArgumentException("Repository spec not supported: " + repoSpec);
    }
  }

  private WarcArtifactDataStore createWarcArtifactDataStore(String dsType) throws Exception {
    log.trace("dsType = {}", dsType);

    if (StringUtil.isNullString(dsType)) {
      return null;
    }

    switch (dsType) {
      case "volatile":
        log.info("Configuring volatile artifact data store");
        return new VolatileWarcArtifactDataStore(index);

      case "local":
      case "testing":
        String baseDirsProp = env.getProperty(LOCAL_BASEDIRS_KEY);

        if (baseDirsProp == null) {
          // Fallback to legacy key
          baseDirsProp = env.getProperty(LOCAL_BASEDIRS_FALLBACK_KEY);

          if (baseDirsProp == null) {
            log.error("No local base directories specified");
            throw new IllegalArgumentException("No local base dirs");
          }
        }

        // Multiple base directories may be provided separated by semicolons
        String[] dirs = baseDirsProp.split(";");

        // Convert String paths to File array
        File[] baseDirs = Arrays.stream(dirs)
            .map(File::new)
            .toArray(File[]::new);

        switch (dsType) {
          case "local":
            log.info("Configuring local artifact data store [baseDirs: {}]",
                Arrays.asList(baseDirs));
            return new LocalWarcArtifactDataStore(index, baseDirs);

          case "testing":
            log.info("Configuring testing artifact data store [baseDirs: {}]",
                Arrays.asList(baseDirs));
            return new TestingWarcArtifactDataStore(index, baseDirs);

          default:
            throw new RuntimeException("Shouldn't happen");
        }

      case "hdfs":
        String hdfsServer = env.getProperty(HDFS_SERVER_KEY);
        Path hdfsBaseDir = Paths.get(env.getProperty(HDFS_BASEDIR_KEY));

        log.info(
            "Configuring HDFS artifact data store [hdfsServer: {}, hdfsBaseDir: {}]",
            hdfsServer, hdfsBaseDir
        );

        HadoopConfigBuilder config = new HadoopConfigBuilder();
        config.fileSystemUri(hdfsServer);

        return new HdfsWarcArtifactDataStore(index, config.build(), hdfsBaseDir);

      default:
        log.error("Unknown artifact data store: '{}'", dsType);
        throw new IllegalArgumentException("Unknown artifact data store");
    }
  }

  // Register config callback for WarcArtifactDataStore after LockssDaemon is started
  @EventListener(ApplicationReadyEvent.class)
  public void registerConfigCallback() {
    LockssApp.getLockssApp()
        .getConfigManager()
        .registerConfigurationCallback(new ArtifactDataStoreConfigCallback(ds));
  }

  /**
   * Configuration callback to set free space map for testing.
   * Configuration mechanism isn't visible to repocore so config callback
   * must be here.
   */
  private static class ArtifactDataStoreConfigCallback
      implements org.lockss.config.Configuration.Callback {

    TestingWarcArtifactDataStore twads;
    WarcArtifactDataStore wads;

    ArtifactDataStoreConfigCallback(WarcArtifactDataStore ds) {
      if (ds instanceof TestingWarcArtifactDataStore) {
        twads = (TestingWarcArtifactDataStore) ds;
      }

      wads = ds;
    }

    public void configurationChanged(org.lockss.config.Configuration newConfig,
                                     org.lockss.config.Configuration oldConfig,
                                     org.lockss.config.Configuration.Differences changedKeys) {

      if (twads != null) {
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

      boolean useWarcCompression =
          newConfig.getBoolean(PARAM_REPO_USE_WARC_COMPRESSION, DEFAULT_REPO_USE_WARC_COMPRESSION);

      wads.setUseWarcCompression(useWarcCompression);
    }
  }
}
