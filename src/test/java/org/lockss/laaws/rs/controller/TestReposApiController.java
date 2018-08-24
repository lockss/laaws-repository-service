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

package org.lockss.laaws.rs.controller;

import org.apache.commons.collections4.IterableUtils;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicStatusLine;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.core.VolatileLockssRepository;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.util.test.LockssTestCase5;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class TestReposApiController extends LockssTestCase5 {
  private static final Logger log = LoggerFactory.getLogger(TestReposApiController.class);

  // The port that Tomcat is using during this test
  @LocalServerPort
  private int port;

  // The Spring application context used to specify the command line arguments to be used for the tests
  @Autowired
  ApplicationContext appContext;

  @Autowired
  private TestRestTemplate restTemplate;

  private static class ReadiableVolatileLockssRepository extends VolatileLockssRepository {
    boolean ready = false;

    public void setReady(boolean ready) {
      this.ready = ready;
    }

    @Override
    public boolean isReady() {
      return ready;
    }
  }

  @TestConfiguration
  public static class TestLockssRepositoryConfiguration {
    @Bean
    public LockssRepository createRepository() {
      return new ReadiableVolatileLockssRepository();
    }
  }

  @Test
  public void deleteArtifactTest() throws Exception {
    // Get a handle to the internal repository
    ReadiableVolatileLockssRepository repository =
        (ReadiableVolatileLockssRepository) appContext.getBean(LockssRepository.class);

    // Verify that the controller returns a 503 Unavailable at this endpoint if the internal repository is not ready
    repository.setReady(false);
    assertFalse(repository.isReady());
    runTestUnavailable("/collections/x/artifacts/y", HttpMethod.DELETE);

    // Proceed with the remainder of this test with a ready internal repository
    repository.setReady(true);
    assertTrue(repository.isReady());

    // Assert empty repository
    assertEquals(0, IterableUtils.size(repository.getCollectionIds()));
    assertEquals(0, runTestGetCollections(HttpStatus.OK).size());

    // Controller should return 404 if we attempt to delete a non-existent artifact
    runTestDeleteArtifact("c1","a1", HttpStatus.NOT_FOUND);

    // Add two committed artifacts
    Artifact a1 = repository.addArtifact(randomArtifactData("c1", "a1"));
    Artifact a2 = repository.addArtifact(randomArtifactData("c1", "a1"));
    repository.commitArtifact(a1);
    repository.commitArtifact(a2);

    // Delete an artifact through the controller then try to delete it again
//    runTestDeleteArtifact(a1.getCollection(), a1.getAuid(), HttpStatus.OK);
//    runTestDeleteArtifact(a1.getCollection(), a1.getAuid(), HttpStatus.NOT_FOUND);

    // Cleanup after ourselves
    repository.deleteArtifact(a1);
    repository.deleteArtifact(a2);
  }

  @Test
  public void getCollectionsTest() throws Exception {
    // Get a handle to the internal repository
    ReadiableVolatileLockssRepository repository =
        (ReadiableVolatileLockssRepository) appContext.getBean(LockssRepository.class);

    // Verify that the controller returns a 503 Unavailable at this endpoint if the internal repository is not ready
    repository.setReady(false);
    repository.setReady(false);
    assertFalse(repository.isReady());
    runTestUnavailable("/collections", HttpMethod.GET);

    // Proceed with the remainder of this test with a ready internal repository
    repository.setReady(true);
    assertTrue(repository.isReady());

    // Assert empty repository
    assertEquals(0, IterableUtils.size(repository.getCollectionIds()));
    assertEquals(0, runTestGetCollections(HttpStatus.OK).size());

    // Populate the internal repository with random committed artifacts
    Set<Artifact> committedArtifacts = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      Artifact artifact = repository.addArtifact(randomArtifactData());
      repository.commitArtifact(artifact);
      committedArtifacts.add(artifact);
    }

    log.info("WOLF: " + runTestGetCollections(HttpStatus.OK).toString());
  }

  private ArtifactData randomArtifactData() {
    StatusLine status = new BasicStatusLine(new ProtocolVersion("HTTP", 1,1), 200, "OK");

    return makeArtifactData(
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString(),
        (long) (Math.random() * Long.MAX_VALUE),
        UUID.randomUUID().toString().getBytes(),
        status
    );
  }

  private ArtifactData randomArtifactData(String collection, String auid) {
    StatusLine status = new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK");

    return makeArtifactData(
        collection,
        auid,
        UUID.randomUUID().toString(),
        (long) (Math.random() * Long.MAX_VALUE),
        UUID.randomUUID().toString().getBytes(),
        status
    );
  }

  private ArtifactData makeArtifactData(String collection, String auid, String url, long version, byte[] data, StatusLine status) {
    ArtifactIdentifier id = new ArtifactIdentifier(collection, auid, url, (int)version);
    InputStream inputStream = new ByteArrayInputStream(data);
    return new ArtifactData(id, null, inputStream, status);
  }

  private void runTestUnavailable(String relUrl, HttpMethod method) {
    // Build a URI to the REST endpoint
    URI uri = UriComponentsBuilder.fromUriString(getTestUrlTemplate(relUrl)).build().encode().toUri();

    // Perform the REST operation and assert service unavailable response status
    ResponseEntity response  = restTemplate.exchange(uri, method, null, Void.class);
    assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.getStatusCode());
  }

  private List<String> runTestGetCollections(HttpStatus expectedStatus) throws IOException {
    // Build a URI to the REST endpoint
    URI uri = UriComponentsBuilder.fromUriString(getTestUrlTemplate("/collections")).build().encode().toUri();

    // Perform the REST operation and get a response from the controller
    ResponseEntity<List<String>> response = restTemplate.exchange(
        uri,
        HttpMethod.GET,
        null,
        new ParameterizedTypeReference<List<String>>() {
        }
    );

    // Assert expected status
    HttpStatus status = response.getStatusCode();
    assertEquals(expectedStatus, status);

    // Return the list of collection IDs
    if (status.is2xxSuccessful()) {
      return response.getBody();
    }

    return null;
  }

  private void runTestDeleteArtifact(String collectionId, String artifactId, HttpStatus expectedStatus) {
    // Build map of URL template key-value pairs
    Map<String,String> templateMap = new HashMap<>();
    templateMap.put("collectionid", collectionId);
    templateMap.put("artifactid", artifactId);

    // Build URI using template
    String template = getTestUrlTemplate("/collections/{collectionid}/artifacts/{artifactid}");
    URI uri = UriComponentsBuilder.fromUriString(template).build().expand(templateMap).encode().toUri();

    // Build request entity
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    HttpEntity requestEntity = new HttpEntity<Void>( null, headers);

    // Perform request and get a response
    ResponseEntity<Void> response = restTemplate.exchange(uri, HttpMethod.DELETE, requestEntity, Void.class);

    // Assert that the return status matches the expected status
    HttpStatus statusCode = response.getStatusCode();
    assertEquals(expectedStatus, statusCode);
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

