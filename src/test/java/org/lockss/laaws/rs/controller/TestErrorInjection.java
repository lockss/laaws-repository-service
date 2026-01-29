/*
Copyright (c) 2000-2026, Board of Trustees of Leland Stanford Jr. University

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

import org.apache.commons.lang3.StringUtils;
import org.junit.*;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.lockss.log.L4JLogger;
import org.lockss.repository.RepositoryDbManager;
import org.lockss.spring.test.SpringLockssTestCase4;
import org.lockss.test.*;
import org.lockss.util.io.FileUtil;
import org.lockss.util.rest.exception.LockssRestHttpException;
import org.lockss.util.rest.repo.LockssRepository;
import org.lockss.util.rest.repo.RestLockssRepository;
import org.lockss.util.rest.repo.model.*;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.test.FileTestUtil;
import org.mockserver.integration.ClientAndServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;

import static org.lockss.app.LockssApp.PARAM_SERVICE_BINDINGS;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;

/**
 * These tests can't be put in TestRestLockssRepository because it
 * uses testing-only Configuration class, but error injection relies
 * on setup in LockssRepositoryConfig
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class TestErrorInjection extends SpringLockssTestCase4 {

  private ClientAndServer mockServer;

  private final static L4JLogger log = L4JLogger.getLogger();

  static List<File> tmpDirs = new ArrayList<>();

  @DynamicPropertySource 
  static void dynamicProperties(DynamicPropertyRegistry registry) { 
    try {
      File stateDir = FileUtil.createTempDir("repostate", "");
      tmpDirs.add(stateDir);
      registry.add("repo.state.dir", () -> stateDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void deleteTempDirs() throws Exception {
    LockssTestCase4.deleteTempFiles(tmpDirs);
  }

  protected static String NS1 = "ns1";
  protected static String AUID1 = "auid1";

  // Credentials.
  private final Credentials USER_ADMIN =
      this.new Credentials("lockss-u", "lockss-p");

  @LocalServerPort
  private int port;

  // The application Context used to specify the command line arguments to be
  // used for the tests.
  @Autowired
  ApplicationContext appCtx;

  @Autowired
  LockssRepository internalRepo;

  private String tempDirPath;
  private String dbPort;

  protected RestLockssRepository repoClient;

  @Before
  public void setUpArtifactDataStore() throws Exception {
    log.fatal("@Before");
    // Get the temporary directory used during the test.
    tempDirPath = setUpDiskSpace();

    dbPort = Integer.toString(TcpTestUtil.findUnboundTcpPort());
    ConfigurationUtil.addFromArgs(RepositoryDbManager.PARAM_DATASOURCE_PORTNUMBER,
        dbPort);

    setupMockServerClient();
    setupLockssApp();
    setupRepositoryClient(USER_ADMIN);
  }

  public void setupMockServerClient() throws IOException {
    int mockServerPort = TcpTestUtil.findUnboundTcpPort();
    mockServer = startClientAndServer(mockServerPort);

    String cfgSvcBinding = "cfg=localhost:" + mockServerPort + ":" + mockServerPort;
//    ConfigurationUtil.addFromArgs(PARAM_SERVICE_BINDINGS, cfgSvcBinding);

    try {
      addParamToMiscConfig(PARAM_SERVICE_BINDINGS, cfgSvcBinding);
      // addToMiscConfig("org.lockss.log.RestServicesManager", "DEBUG2");
    } catch (IOException e) {
      log.error("Could not add misc configuration parameters", e);
      throw e;
    }
  }

  @Override
  protected MockLockssDaemon newMockLockssDaemon() {
    return null;
  }

  private void setupRepositoryClient(Credentials crd) throws IOException {
    repoClient = new RestLockssRepository(
        new URL(String.format("http://localhost:%d", port)), crd.getUser(), crd.getPassword());
  }
  
  public void setupLockssApp() throws Exception {
    // Set up the temporary directory where the test data will reside.
    setUpTempDirectory(this.getClass().getCanonicalName());

    // Set up the UI port.
    setUpUiPort(UI_PORT_CONFIGURATION_TEMPLATE, UI_PORT_CONFIGURATION_FILE);
    
    // Specify the command line parameters to be used for the tests.
    List<String> cmdLineArgs = getCommandLineArguments();
    cmdLineArgs.add("-p");
    cmdLineArgs.add("test/config/testAuthOn.txt");

    // This is a one way to configure REST credentials so that this service can 
    // make REST calls to other services. The "-s" mechanism was intended for
    // Kubernetes secrets and the file will be deleted after being read.
    File lockssAuth =
        FileTestUtil.writeTempFile("lockss-auth", "lockss-u:lockss-p");
    cmdLineArgs.add("-s");
    cmdLineArgs.add("rest:" + lockssAuth);

    log.info("cmdLineArgs: " + cmdLineArgs);

    // XXX This is kinda wonky.  SpringRunner has already set up the
    // test environment; this starts (parts of?) it over again
    CommandLineRunner runner = appCtx.getBean(CommandLineRunner.class);
    runner.run(cmdLineArgs.toArray(new String[cmdLineArgs.size()]));
  }

  /**
   * Provides the standard command line arguments to start the server.
   *
   * @return a {@code List<String>} with the command line arguments.
   */
  private List<String> getCommandLineArguments() {
    log.debug2("Invoked");

    List<String> cmdLineArgs = new ArrayList<String>();
    cmdLineArgs.add("-p");
    cmdLineArgs.add(getPlatformDiskSpaceConfigPath());
    cmdLineArgs.add("-p");
    cmdLineArgs.add(getUiPortConfigFile().getAbsolutePath());
    cmdLineArgs.add("-p");
    cmdLineArgs.add("test/config/lockss.txt");
    cmdLineArgs.add("-p");
    cmdLineArgs.add("test/config/lockss.opt");
    cmdLineArgs.add("-p");
    cmdLineArgs.add(getMiscConfigPath());

    log.debug2("cmdLineArgs = {}", cmdLineArgs);
    return cmdLineArgs;
  }

  @After
  public void tearDownArtifactDataStore() throws Exception {
    mockServer.stop();
    this.repoClient = null;
  }


  @Test
  public void testErrorInjection() throws Exception {
    String rules = """
      {"cond": { "op" : "AddArtifact", "uri":".*path1.*"},
       "action":{"ex":"IOException", "msg":"path1 add error"}
      };
    {"cond": { "op" : "CommitArtifact", "uri":".*path2.*", "ords":"2,4"},
        "action":{"ex":"IOException", "msg":"path2 commit error"}
    };
    {"cond": { "op" : "GetArtifact", "uri":".*path2.*", "ords":"2"},
        "action":{"ex":"IOException", "msg":"path2 get error"}
    }
    """;

    ConfigurationUtil.addFromArgs(org.lockss.laaws.rs.configuration.LockssRepositoryConfig.PARAM_ERROR_INJECTION_SPEC, rules.replaceAll("\n", " "));

    // This one should work
    ArtifactSpec spec1 = ArtifactSpec.forNsAuUrl(NS1, AUID1, "http://path333/foo")
      .toCommit(true).setContentLength(10);
    Artifact art1 = addUncommitted(spec1);
    Artifact cart1 = commit(spec1, art1);
    spec1.assertArtifact(repoClient, cart1);

    // Should fail on add
    ArtifactSpec spec2 = ArtifactSpec.forNsAuUrl(NS1, AUID1, "http://path1/foo")
      .toCommit(true).setContentLength(10);
    try {
      Artifact art2 = addUncommitted(spec2);
    } catch (LockssRestHttpException e) {
      assertEquals("500 Internal Server Error: Could not add artifact to remote repository",
                   e.getMessage());
      assertEquals("path1 add error", e.getServerErrorMessage());
    }

    // Should fail on commit
    ArtifactSpec spec3 = ArtifactSpec.forNsAuUrl(NS1, AUID1, "http://path2/foo")
      .toCommit(true).setContentLength(10);
    Artifact art3a = addUncommitted(spec3);
    // 1st commit should work
    Artifact cart3a = commit(spec3, art3a);
    // 2nd & 4th should throw
    spec3.setCommitted(false);
    Artifact art3b = addUncommitted(spec3);
    try {
      Artifact cart3b = commit(spec3, art3b);
    } catch (LockssRestHttpException e) {
      assertEquals("500 Internal Server Error: commitArtifact client error",
                   e.getMessage());
      assertEquals("path2 commit error", e.getServerErrorMessage());
    }
    Artifact art3c = addUncommitted(spec3);
    Artifact cart3c = commit(spec3, art3c);
    spec3.setCommitted(false);
    Artifact art3d = addUncommitted(spec3);
    try {
      Artifact cart3d = commit(spec3, art3d);
    } catch (LockssRestHttpException e) {
      assertEquals("500 Internal Server Error: commitArtifact client error",
                   e.getMessage());
      assertEquals("path2 commit error", e.getServerErrorMessage());
    }
    // There's already been one get of this artifact.  This is number
    // 2 and should fail
    assertThrowsMatch(LockssRestHttpException.class,
                      "500 Internal Server Error: getArtifact",
                      () -> repoClient.getArtifact(spec3.getNamespace(), spec3.getAuid(), spec3.getUrl()));
    Artifact a3a = repoClient.getArtifact(spec3.getNamespace(), spec3.getAuid(), spec3.getUrl());
    assertNotNull(a3a);
    spec3.setCommitted(true);
    spec3.assertArtifact(repoClient, a3a);
  }

  Artifact addUncommitted(ArtifactSpec spec) throws IOException {
    if (!spec.hasContent()) {
      spec.generateContent();
    }
    log.debug2("adding: " + spec);

    ArtifactData ad = spec.getArtifactData();
    Artifact newArt = repoClient.addArtifact(ad);
    assertNotNull(newArt);

    spec.assertArtifact(repoClient, newArt);
    return newArt;
  }

  Artifact commit(ArtifactSpec spec, Artifact art) throws IOException {
    String artUuid = art.getUuid();
    log.debug2("committing: " + art);
    Artifact commArt = repoClient.commitArtifact(spec.getNamespace(), artUuid);
    assertNotNull(commArt);

    if (spec.getExpVer() > 0) {
      assertEquals(spec.getExpVer(), (int) commArt.getVersion());
    }
    spec.setCommitted(true);
    assertTrue(commArt.getCommitted());

    spec.assertArtifact(repoClient, commArt);

    return commArt;
  }


}
