/*

Copyright (c) 2000-2024, Board of Trustees of Leland Stanford Jr. University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/
package org.lockss.laaws.rs.controller;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import jakarta.annotation.PreDestroy;
import org.lockss.config.ConfigManager;
import org.lockss.config.CurrentConfig;
import org.lockss.db.DbException;
import org.lockss.db.DbManager;
import org.lockss.log.L4JLogger;
import org.lockss.repository.RepositoryDbManager;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.rs.io.index.db.SQLArtifactIndex;
import org.lockss.rs.io.index.db.SQLArtifactIndexDbManager;
import org.lockss.rs.io.storage.ArtifactDataStore;
import org.lockss.rs.io.storage.warc.LocalWarcArtifactDataStore;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.MockLockssDaemon;
import org.lockss.test.TcpTestUtil;
import org.lockss.util.FileUtil;
import org.lockss.util.StringUtil;
import org.lockss.util.rest.repo.LockssRepository;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.lockss.test.LockssTestCase4.getTempDir;
import static org.mockito.Mockito.spy;

/**
 * Experimental test Spring configuration used to allow tests to test against an
 * embedded PostgreSQL database. An important caveat is that embedded PostgreSQL
 * database is associated with the Spring Application Context and is not
 * reinitialized between unit tests.
 */
@TestConfiguration
public class SQLTestRepositoryApplicationConfiguration {
  private final static L4JLogger log = L4JLogger.getLogger();

  private final ApplicationContext appCtx;
  private MockLockssDaemon theDaemon;
  private SQLArtifactIndexDbManager idxDbManager;
  EmbeddedPostgres embeddedPg;

  static List<File> tmpDirs = new ArrayList<>();
  private String dbPort;

  @Autowired
  public SQLTestRepositoryApplicationConfiguration(ApplicationContext appCtx) {
    this.appCtx = appCtx;
  }

  @PreDestroy
  public void release() throws Exception {
    LockssTestCase4.deleteTempFiles(tmpDirs);
  }

  @Bean
  public LockssRepository lockssRepository(
      @Autowired ArtifactIndex index,
      @Autowired ArtifactDataStore ds
  ) throws IOException {
    File stateDir = getTempDir(tmpDirs);

    LockssRepository repository =
        new BaseLockssRepository(stateDir, index, ds);

    return spy(repository);
  }

  @Bean
  public ArtifactDataStore artifactDataStore() throws IOException {
    File basePath = getTempDir(tmpDirs);
    return new LocalWarcArtifactDataStore(basePath);
  }

  @Bean
  public ArtifactIndex artifactIndex() throws DbException {
      ConfigManager mgr = ConfigManager.makeConfigManager(appCtx);
      ConfigManager.setConfigManager(mgr, appCtx);

      theDaemon = new MyMockLockssDaemon();
      theDaemon.setAppRunning(true);
      theDaemon.setDaemonInited(true);

      dbPort = Integer.toString(TcpTestUtil.findUnboundTcpPort());
      ConfigurationUtil.addFromArgs(RepositoryDbManager.PARAM_DATASOURCE_PORTNUMBER,
          dbPort);

    try {
      setUpDiskSpace();
      initializePostgreSQL();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to start embedded PostgreSQL", e);
    }

    return new SQLArtifactIndex();
  }

  private String setUpDiskSpace() throws IOException {
    String diskList =
        CurrentConfig.getParam(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST);
    if (!StringUtil.isNullString(diskList)) {
      return StringUtil.breakAt(diskList, ";").get(0);
    }
    String tmpdir = getTempDir(tmpDirs).getAbsolutePath() + File.separator;
    ConfigurationUtil.addFromArgs(ConfigManager.PARAM_PLATFORM_DISK_SPACE_LIST,
        tmpdir);
    return tmpdir;
  }

  private static class MyMockLockssDaemon extends MockLockssDaemon {
    // Intentionally left blank
  }

  private void initializePostgreSQL() throws Exception {
    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_USER, "postgres",
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PASSWORD, "postgresx");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.DATASOURCE_ROOT + ".dbcp.enabled", "true",
        SQLArtifactIndexDbManager.DATASOURCE_ROOT + ".dbcp.initialSize", "2");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_MAX_RETRY_COUNT, "0",
        SQLArtifactIndexDbManager.PARAM_RETRY_DELAY, "0");

    ConfigurationUtil.addFromArgs(
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_CLASSNAME, PGSimpleDataSource.class.getCanonicalName(),
        SQLArtifactIndexDbManager.PARAM_DATASOURCE_PASSWORD, "postgres");

    idxDbManager = new SQLArtifactIndexDbManager();
    startEmbeddedPgDbManager(idxDbManager);
    idxDbManager.initService(theDaemon);

    idxDbManager.setTargetDatabaseVersion(4);
    idxDbManager.startService();

    theDaemon.setSQLArtifactIndexDbManager(idxDbManager);
  }

  private void startEmbeddedPgDbManager(DbManager mgr) throws DbException {
    try {
      if (embeddedPg == null) {
        EmbeddedPostgres.Builder builder = EmbeddedPostgres.builder();
        String extemp = System.getProperty("org.lockss.executableTempDir");
        if (!StringUtil.isNullString(extemp)) {
          builder.setOverrideWorkingDirectory(new File(extemp));
        }
        embeddedPg = builder.start();
      }
      String dbName = mgr.getDatabaseNamePrefix()
          + mgr.getClass().getSimpleName();
      mgr.setTestingDataSource(embeddedPg.getDatabase("postgres", dbName));
    } catch (IOException e) {
      throw new DbException("Can't start embedded PostgreSQL", e);
    }
  }
}
