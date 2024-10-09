/*

Copyright (c) 2019 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */
package org.lockss.laaws.rs.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.lockss.app.LockssApp;
import org.lockss.app.LockssDaemon;
import org.lockss.laaws.rs.controller.DefaultTestRepositoryApplicationConfiguration;
import org.lockss.log.L4JLogger;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.index.AbstractArtifactIndex;
import org.lockss.rs.io.index.VolatileArtifactIndex;
import org.lockss.spring.test.SpringLockssTestCase4;
import org.lockss.util.rest.repo.LockssRepository;
import org.lockss.util.rest.status.ApiStatus;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Test class for org.lockss.laaws.mdq.api.MetadataApiServiceImpl and
 * org.lockss.laaws.mdq.api.UrlsApiServiceImpl.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = { DefaultTestRepositoryApplicationConfiguration.class })
public class TestStatusApiServiceImpl extends SpringLockssTestCase4 {
  private static L4JLogger log = L4JLogger.getLogger();

  // The port that Tomcat is using during this test.
  @LocalServerPort
  private int port;

  // The application Context used to specify the command line arguments to be
  // used for the tests.
  @Autowired
  ApplicationContext appCtx;

  /**
   * Set up code to be run before each test.
   *
   * @throws IOException if there are problems.
   */
  @Before
  public void setUpBeforeEachTest() throws IOException {
    log.debug2("port = {}", port);

    getMockLockssDaemon().setAppRunning(true);
    repo.initRepository();

    // Set up the temporary directory where the test data will reside.
    setUpTempDirectory(TestStatusApiServiceImpl.class.getCanonicalName());

    // Set up the UI port.
    setUpUiPort(UI_PORT_CONFIGURATION_TEMPLATE, UI_PORT_CONFIGURATION_FILE);

    log.debug2("Done");
  }

  /**
   * Runs the full controller tests with authentication turned off.
   * 
   * @throws Exception
   *           if there are problems.
   */
  @Test
  public void runUnAuthenticatedTests() throws Exception {
    log.debug2("Invoked");

    // Specify the command line parameters to be used for the tests.
    List<String> cmdLineArgs = getCommandLineArguments();
    cmdLineArgs.add("-p");
    cmdLineArgs.add("test/config/testAuthOff.txt");

    CommandLineRunner runner = appCtx.getBean(CommandLineRunner.class);
    runner.run(cmdLineArgs.toArray(new String[cmdLineArgs.size()]));

    getStatusTest();

    log.debug2("Done");
  }

  /**
   * Runs the full controller tests with authentication turned on.
   *
   * @throws Exception
   *           if there are problems.
   */
  @Test
  public void runAuthenticatedTests() throws Exception {
    log.debug2("Invoked");

    // Specify the command line parameters to be used for the tests.
    List<String> cmdLineArgs = getCommandLineArguments();
    cmdLineArgs.add("-p");
    cmdLineArgs.add("test/config/testAuthOn.txt");

    CommandLineRunner runner = appCtx.getBean(CommandLineRunner.class);
    runner.run(cmdLineArgs.toArray(new String[cmdLineArgs.size()]));

    getStatusTest();

    log.debug2("Done");
  }

  /**
   * Provides the standard command line arguments to start the server.
   *
   * @return a List<String> with the command line arguments.
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

    log.debug2("cmdLineArgs = {}", cmdLineArgs);
    return cmdLineArgs;
  }

  @Autowired
  LockssRepository repo;

  /**
   * Runs the status-related tests.
   *
   * @throws JsonProcessingException
   *           if there are problems getting the expected status in JSON format.
   */
  private void getStatusTest() throws JsonProcessingException {
    log.debug2("Invoked");

    ResponseEntity<String> successResponse = new TestRestTemplate().exchange(
	getTestUrlTemplate("/status"), HttpMethod.GET, null, String.class);

    HttpStatusCode statusCode = successResponse.getStatusCode();
    HttpStatus status = HttpStatus.valueOf(statusCode.value());
    assertEquals(HttpStatus.OK, status);

    // Get the expected result.
    ApiStatus expected = new ApiStatus("swagger/swagger.yaml");
    expected.setReady(true);
    expected.setReadyTime(LockssApp.getLockssApp().getReadyTime());
    expected.setStartupStatus(LockssDaemon.getLockssDaemon().getStartupStatus());

    JSONAssert.assertEquals(expected.toJson(), successResponse.getBody(),
	false);

    // ensure that repo.isReady() is included in ApiStatus.getReady()
    // (lots of assumptions here about the implementation classes used
    // in the test.)
    VolatileArtifactIndex index =
      (VolatileArtifactIndex)((BaseLockssRepository)repo).getArtifactIndex();
    index.setState(AbstractArtifactIndex.ArtifactIndexState.STOPPED);
    ResponseEntity<String> resp2 = new TestRestTemplate().exchange(
	getTestUrlTemplate("/status"), HttpMethod.GET, null, String.class);
    assertEquals(HttpStatus.OK, resp2.getStatusCode());

    expected.setReady(false);
    JSONAssert.assertEquals(expected.toJson(), resp2.getBody(), false);
    index.setState(AbstractArtifactIndex.ArtifactIndexState.RUNNING);

    log.debug2("Done");
  }

  /**
   * Provides the URL template to be tested.
   *
   * @param pathAndQueryParams
   *          A String with the path and query parameters of the URL template to
   *          be tested.
   * @return a String with the URL template to be tested.
   */
  private String getTestUrlTemplate(String pathAndQueryParams) {
    return "http://localhost:" + port + pathAndQueryParams;
  }
}
