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

import org.lockss.app.LockssDaemon;
import org.lockss.jms.JMSManager;
import org.lockss.laaws.rs.core.BaseLockssRepository;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.core.RestLockssRepository;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.util.JmsFactorySource;
import org.lockss.log.L4JLogger;
import org.lockss.util.Deadline;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;

/**
 * Spring configuration beans for the configuration of the Repository Service's internal repository.
 */
@Configuration
public class LockssRepositoryConfig {
  private final static L4JLogger log = L4JLogger.getLogger();

  private final RepositoryServiceProperties repoProps;
  private ArtifactDataStore store;
  private ArtifactIndex index;

  @Autowired
  public LockssRepositoryConfig(RepositoryServiceProperties repoProps, ArtifactIndex index, ArtifactDataStore store) {
    this.repoProps = repoProps;
    this.index = index;
    this.store = store;
  }

  /**
   * Creates and initializes the {@link LockssRepository} that will be made available via the
   * LOCKSS Repository Service's REST API.
   *
   * @return An initialized {@link LockssRepository}.
   * @throws IOException
   */
  @Bean
  public LockssRepository createInitializedRepository() throws IOException {
    // Create and initialize the LockssRepository instance
    LockssRepository repo = createLockssRepository();

    repo.initRepository();

    // Start a new thread to handle JMS messages to this LockssRepository
    // XXX wrong method, below is public
    new Thread(() -> initJmsFactory(repo)).start();

    return repo;
  }

  /**
   * Creates a {@link LockssRepository}
   *
   * @return
   * @throws IOException
   */
  public LockssRepository createLockssRepository() throws IOException {
    log.debug("Starting internal LOCKSS repository [repoSpec: {}]", repoProps.getRepositorySpec());

    switch (repoProps.getRepositoryType()) {
      case "volatile":
      case "local":
      case "custom":
        // Local state directory for this Repository Service
        File stateDir = repoProps.getRepositoryStateDir();

        // Configure artifact index and data store individually using Spring beans (see their
        // configuration beans in) the ArtifactIndexConfig and ArtifactDataStoreConfig classes,
        // respectively.
        return new BaseLockssRepository(stateDir, index, store);

      case "rest":
        if (repoProps.getRepoSpecParts().length <= 1) {
          log.error("No REST endpoint specified");
          throw new IllegalArgumentException("No REST endpoint specified");
        }

        String repositoryRestUrl = repoProps.getRepoSpecParts()[1];

        log.debug("repositoryRestUrl = {}", repositoryRestUrl);

        // Get the REST client credentials.
        List<String> restClientCredentials =
            LockssDaemon.getLockssDaemon().getRestClientCredentials();

        log.trace("restClientCredentials = {}", restClientCredentials);

        String userName = null;
        String password = null;

        // Check whether there is a user name.
        if (restClientCredentials != null && restClientCredentials.size() > 0) {
          // Yes: Get the user name.
          userName = restClientCredentials.get(0);

          log.trace("userName = " + userName);

          // Check whether there is a user password.
          if (restClientCredentials.size() > 1) {
            // Yes: Get the user password.
            password = restClientCredentials.get(1);
          }

          // Check whether no configured user was found.
          if (userName == null || password == null) {
            String errMsg = "No user has been configured for authentication";
            log.error(errMsg);
            throw new IllegalArgumentException(errMsg);
          }
        }

        return new RestLockssRepository(new URL(repositoryRestUrl), userName, password);

      default:
        // Unknown repository specification
        String errMsg = String.format("Unknown repository specification '%s'", repoProps.getRepositorySpec());
        log.error(errMsg);
        throw new IllegalArgumentException(errMsg);
    }
  }

  /**
   * Wait for LockssDaemon to be instantiated, then to become ready, then
   * obtain a JmsFactory from JMSManager and store it in the
   * recently-created LockssRepositor.  Runs in a thread started by the
   * LockssRepository bean above
   */
  void initJmsFactory(LockssRepository repo) {
    if (repo instanceof JmsFactorySource) {
      JmsFactorySource jmsSource = (JmsFactorySource) repo;
      LockssDaemon daemon = null;
      while (daemon == null) {
        try {
          daemon = LockssDaemon.getLockssDaemon();
        } catch (IllegalStateException e) {
          log.warn("getLockssDaemon() timed out");
        }
      }
      try {
        while (!daemon.waitUntilAppRunning(Deadline.in(5 * 60 * 1000))) ;
        JMSManager mgr = daemon.getManagerByType(JMSManager.class);
        jmsSource.setJmsFactory(mgr.getJmsFactory());
        log.info("Stored JmsFactory in {}", jmsSource);
      } catch (IllegalArgumentException e) {
        log.warn("Couldn't get JmsManager", e);
      } catch (InterruptedException e) {
        // exit
      }
    }
  }
}
