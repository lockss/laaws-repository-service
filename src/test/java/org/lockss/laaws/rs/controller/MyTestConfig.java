package org.lockss.laaws.rs.controller;

import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.rs.io.index.db.SQLArtifactIndex;
import org.lockss.rs.io.storage.ArtifactDataStore;
import org.lockss.rs.io.storage.warc.LocalWarcArtifactDataStore;
import org.lockss.test.LockssTestCase4;
import org.lockss.util.rest.repo.LockssRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.spy;

@TestConfiguration
public class MyTestConfig {
  static List<File> tmpDirs = new ArrayList<>();

  public MyTestConfig() { }
  /**
   * Initializes the internal {@link LockssRepository} instance used by the
   * embedded LOCKSS Repository Service.
   */
  @Bean
  public LockssRepository createInitializedRepository(
      @Autowired ArtifactIndex index,
      @Autowired ArtifactDataStore ds
  ) throws IOException {
    File stateDir = LockssTestCase4.getTempDir(tmpDirs);

    LockssRepository repository =
        new BaseLockssRepository(stateDir, index, ds);

    return spy(repository);
//    LockssRepository repo = mock(LockssRepository.class);
//
//    when(repo.isReady()).thenReturn(true);
//
//    return repo;
  }

  @Bean
  public ArtifactIndex setArtifactIndex() {
    return new SQLArtifactIndex();
  }

  @Bean
  public ArtifactDataStore setArtifactDataStore() throws IOException {
    File basePath = LockssTestCase4.getTempDir(tmpDirs);
    return new LocalWarcArtifactDataStore(basePath);
  }
}