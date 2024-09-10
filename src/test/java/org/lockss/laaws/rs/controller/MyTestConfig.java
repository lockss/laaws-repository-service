package org.lockss.laaws.rs.controller;

import org.lockss.rs.LocalLockssRepository;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.rs.io.index.VolatileArtifactIndex;
import org.lockss.rs.io.storage.ArtifactDataStore;
import org.lockss.rs.io.storage.warc.VolatileWarcArtifactDataStore;
import org.lockss.test.LockssTestCase4;
import org.lockss.util.rest.repo.LockssRepository;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.lockss.laaws.rs.controller.TestRestLockssRepository.NS1;
import static org.mockito.Mockito.*;

@TestConfiguration
public class MyTestConfig {
  static List<File> tmpDirs = new ArrayList<>();

  public MyTestConfig() { }
  /**
   * Initializes the internal {@link LockssRepository} instance used by the
   * embedded LOCKSS Repository Service.
   */
  @Bean
  public LockssRepository createInitializedRepository() throws IOException {
    File stateDir = LockssTestCase4.getTempDir(tmpDirs);
    File basePath = LockssTestCase4.getTempDir(tmpDirs);

    LockssRepository repository =
        new LocalLockssRepository(stateDir, basePath, NS1);

    return spy(repository);
//    LockssRepository repo = mock(LockssRepository.class);
//
//    when(repo.isReady()).thenReturn(true);
//
//    return repo;
  }

  @Bean
  public ArtifactIndex setArtifactIndex() {
    return new VolatileArtifactIndex();
  }

  @Bean
  public ArtifactDataStore setArtifactDataStore() {
    return new VolatileWarcArtifactDataStore();
  }
}