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

import org.lockss.db.DbException;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.rs.io.index.VolatileArtifactIndex;
import org.lockss.rs.io.storage.ArtifactDataStore;
import org.lockss.rs.io.storage.warc.VolatileWarcArtifactDataStore;
import org.lockss.test.LockssTestCase4;
import org.lockss.util.rest.repo.LockssRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.spy;

@TestConfiguration
public class DefaultTestRepositoryApplicationConfiguration {
  static List<File> tmpDirs = new ArrayList<>();
  private final ApplicationContext appCtx;

  @Autowired
  public DefaultTestRepositoryApplicationConfiguration(ApplicationContext appCtx) {
    this.appCtx = appCtx;
  }

  @Bean
  public LockssRepository lockssRepository(
      @Autowired ArtifactIndex index,
      @Autowired ArtifactDataStore ds
  ) throws IOException {
    File stateDir = LockssTestCase4.getTempDir(tmpDirs);

    LockssRepository repository =
        new BaseLockssRepository(stateDir, index, ds);

    return spy(repository);
  }

  @Bean
  public ArtifactIndex artifactIndex() throws DbException {
    return new VolatileArtifactIndex();
  }

  @Bean
  public ArtifactDataStore artifactDataStore() throws IOException {
    return new VolatileWarcArtifactDataStore();
  }
}