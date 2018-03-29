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
import org.lockss.laaws.rs.core.BaseLockssRepository;
import org.lockss.laaws.rs.core.LocalLockssRepository;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.solr.SolrArtifactIndex;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.io.storage.warc.VolatileWarcArtifactDataStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.net.MalformedURLException;

@Configuration
public class LockssRepositoryConfig {
  private final static Log log =
      LogFactory.getLog(LockssRepositoryConfig.class);

  @Value("${repo.local.path}")
  private String repoLocalPath;

    @Bean
    public LockssRepository createRepository() throws MalformedURLException {
      log.info("repoLocalPath = " + repoLocalPath);

      if (repoLocalPath == null || repoLocalPath.trim().isEmpty()) {
//        ArtifactIndex index = new VolatileArtifactIndex();
        ArtifactIndex index = new SolrArtifactIndex("http://localhost:8983/solr/test");
        ArtifactDataStore store = new VolatileWarcArtifactDataStore();
//        ArtifactDataStore store = new LocalWarcArtifactDataStore(new File("repo"));
        return new BaseLockssRepository(index, store);
      } else {
        LocalLockssRepository localRepo =
            new LocalLockssRepository(new File(repoLocalPath));
        return localRepo;
      }
    }
}
