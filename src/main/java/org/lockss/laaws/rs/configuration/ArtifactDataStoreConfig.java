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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.lockss.config.ConfigManager;
import org.lockss.log.L4JLogger;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.rs.io.storage.ArtifactDataStore;
import org.lockss.rs.io.storage.ArtifactDataStoreVersion;
import org.lockss.rs.io.storage.warc.LocalWarcArtifactDataStore;
import org.lockss.rs.io.storage.warc.TestingWarcArtifactDataStore;
import org.lockss.rs.io.storage.warc.VolatileWarcArtifactDataStore;
import org.lockss.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.util.PatternIntMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import java.io.*;
import java.util.List;

import static org.lockss.rs.io.storage.warc.WarcArtifactDataStore.DATASTORE_VERSION_FILE;

/**
 * Spring configuration beans for the configuration of the Repository Service's internal artifact data store.
 */
@Configuration
public class ArtifactDataStoreConfig {
  private final static L4JLogger log = L4JLogger.getLogger();

  private final static ObjectMapper mapper = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  public final static String PARAM_FREE_SPACE_MAP = "org.lockss.repo.testing.freeSpaceMap";

  /**
   * Enables or disables the use of GZIP compression for WARC files in
   * WARC artifact data store implementations.
   */
  public final static String PARAM_REPO_USE_WARC_COMPRESSION = "org.lockss.repo.warc.useCompression";

  /**
   * Default settings for use of GZIP compression for WARC files.
   */
  public final static boolean DEFAULT_REPO_USE_WARC_COMPRESSION = true;

  private RepositoryServiceProperties repoProps;

  ArtifactIndex index;
  volatile WarcArtifactDataStore ds;

  @Autowired
  public ArtifactDataStoreConfig(RepositoryServiceProperties repoProps, ArtifactIndex index) {
    this.repoProps = repoProps;
    this.index = index;
  }

  @Bean
  public ArtifactDataStore artifactDataStore() throws Exception {
    // Create WARC artifact data store and set use WARC compression
    ds = createWarcArtifactDataStore(parseDataStoreSpecs());
    return ds;
  }

  @Bean
  public ArtifactDataStoreVersion artifactDataStoreVersion() {
    try {
      File versionFile = new File(repoProps.getRepositoryStateDir(), DATASTORE_VERSION_FILE);
      ArtifactDataStoreVersion onDiskVersion = readArtifactDataStoreVersion(versionFile);
      return onDiskVersion;
    } catch (IOException e) {
      throw new IllegalStateException("Couldn't read artifact index version", e);
    }
  }

  private static ArtifactDataStoreVersion readArtifactDataStoreVersion(File versionFile) throws IOException {
    try (InputStream is = new BufferedInputStream(new FileInputStream(versionFile))) {
      return mapper.readValue(is, ArtifactDataStoreVersion.class);
    } catch (FileNotFoundException e) {
      log.debug("Could not find data store version file: " + versionFile);
      return ArtifactDataStoreVersion.UNKNOWN;
    }
  }

  private String parseDataStoreSpecs() {
    switch (repoProps.getRepositoryType()) {
      case "volatile":
        // Allow a volatile data store to be created so that WARC compression can be configured
        return "volatile";

      case "local":
        // Support for legacy repo.spec=local:X;Y;Z parameter
        return "local";

      case "custom":
        return repoProps.getDatastoreSpec();

      default:
        throw new IllegalArgumentException("Repository spec not supported: " + repoProps.getRepositorySpec());
    }
  }

  private WarcArtifactDataStore createWarcArtifactDataStore(String dsType) throws Exception {
    switch (dsType) {
      case "volatile":
        log.info("Configuring volatile artifact data store");
        return new VolatileWarcArtifactDataStore();

      case "local":
      case "testing":
        switch (dsType) {
          case "local":
            log.info("Configuring local artifact data store [baseDirs: {}]", repoProps.getLocalBaseDirs());
            return new LocalWarcArtifactDataStore(repoProps.getLocalBaseDirs());

          case "testing":
            log.info("Configuring testing artifact data store [baseDirs: {}]", repoProps.getLocalBaseDirs());
            return new TestingWarcArtifactDataStore(repoProps.getLocalBaseDirs());

          default:
            throw new RuntimeException("Shouldn't happen");
        }

      default:
        log.error("Unknown artifact data store: '{}'", dsType);
        throw new IllegalArgumentException("Unknown artifact data store");
    }
  }

  // Register config callback for WarcArtifactDataStore once ConfigManager
  // has been created.
  @EventListener
  public void configMgrCreated(ConfigManager.ConfigManagerCreatedEvent event) {
    log.debug2("ConfigManagerCreatedEvent triggered");
    ConfigManager.getConfigManager()
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

      if (wads != null) {
        boolean useWarcCompression =
          newConfig.getBoolean(PARAM_REPO_USE_WARC_COMPRESSION, DEFAULT_REPO_USE_WARC_COMPRESSION);
        wads.setUseWarcCompression(useWarcCompression);
      } else {
        log.warn("configurationChanged() called before ConfigManager started.  Okey while running unit tests, should not happen during real startup");
      }
    }
  }
}
