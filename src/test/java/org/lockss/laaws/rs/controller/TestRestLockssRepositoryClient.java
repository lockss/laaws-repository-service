/*

Copyright (c) 2019-2020 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.laaws.rs.controller;

import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withServerError;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import org.apache.commons.io.IOUtils;
import org.apache.http.ProtocolVersion;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.lockss.laaws.rs.core.LockssNoSuchArtifactIdException;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.core.RestLockssRepository;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.RepositoryArtifactMetadata;
import org.lockss.laaws.rs.util.ArtifactConstants;
import org.lockss.laaws.rs.util.ArtifactDataUtil;
import org.lockss.util.LockssUncheckedException;
import org.lockss.util.rest.RestUtil;
import org.lockss.util.rest.exception.*;
import org.lockss.util.test.LockssTestCase5;
import org.lockss.log.L4JLogger;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

/**
 * Test of the REST repository client.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class TestRestLockssRepositoryClient extends LockssTestCase5 {
    private final static L4JLogger log = L4JLogger.getLogger();
    private final static String BASEURL = "http://localhost:24610";
    protected LockssRepository repository;
    protected MockRestServiceServer mockServer;

    /**
     * Creates the REST repository client to be used in the test.
     * @throws Exception if there are problems.
     */
    @Before
    public void makeLockssRepository() throws Exception {
      RestTemplate restTemplate = RestUtil.getRestTemplate();
      mockServer = MockRestServiceServer.createServer(restTemplate);

      // The authentication credentials.
      String userName = null;
      String password = null;

      repository = new RestLockssRepository(new URL(BASEURL), restTemplate,
	  userName, password);
    }

    @Test
    public void testGetCollectionIds_empty() throws Exception {
        // Test with empty result
        mockServer.expect(requestTo(String.format("%s/collections", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("[]", MediaType.APPLICATION_JSON));

        Iterable<String> collectionIds = repository.getCollectionIds();
        mockServer.verify();

        assertNotNull(collectionIds);
        assertFalse(collectionIds.iterator().hasNext());
    }

    @Test
    public void testGetCollectionIds_success() throws Exception {
        // Test with valid result
        mockServer.expect(requestTo(String.format("%s/collections", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("[\"collection1\"]", MediaType.APPLICATION_JSON));

        Iterable<String> collectionIds = repository.getCollectionIds();
        mockServer.verify();

        assertNotNull(collectionIds);
        assertTrue(collectionIds.iterator().hasNext());
        assertEquals("collection1", collectionIds.iterator().next());
        assertFalse(collectionIds.iterator().hasNext());
    }

    @Test
    public void testGetCollectionIds_failure() throws Exception {
        // Test with server error.
        mockServer.expect(requestTo(String.format("%s/collections", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

	assertThrowsMatch(LockssRestHttpException.class,
		      "500",
		      () -> {repository.getCollectionIds();});
        mockServer.verify();
    }

    @Test
    public void testArtifactExists_iae() throws Exception {
        try {
            repository.artifactExists("collection1", null);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}

        try {
            repository.artifactExists("collection1", "");
            fail("Should have thrown IllegalArgumentException");
      } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testArtifactExists_false() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1", BASEURL)))
                .andExpect(method(HttpMethod.HEAD))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Boolean artifactExists = repository.artifactExists("collection1", "artifact1");
        mockServer.verify();

        assertNotNull(artifactExists);
        assertFalse(artifactExists);
    }

    @Test
    public void testArtifactExists_true() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1", BASEURL)))
                .andExpect(method(HttpMethod.HEAD))
                .andRespond(withSuccess());

        Boolean artifactExists = repository.artifactExists("collection1", "artifact1");
        mockServer.verify();

        assertNotNull(artifactExists);
        assertTrue(artifactExists);
    }

    @Test
    public void testArtifactExists_failure() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1", BASEURL)))
                .andExpect(method(HttpMethod.HEAD))
                .andRespond(withServerError());

	assertThrowsMatch(LockssRestHttpException.class,
		      "500",
			  () -> {repository.artifactExists("collection1", "artifact1");});

        mockServer.verify();
    }

    @Test
    public void testIsArtifactCommitted_iae() throws Exception {
        try {
            repository.isArtifactCommitted("collection1", null);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}

        try {
            repository.isArtifactCommitted("collection1", "");
            fail("Should have thrown IllegalArgumentException");
      } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testIsArtifactCommitted_missingheader() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1", BASEURL)))
                .andExpect(method(HttpMethod.HEAD))
                .andRespond(withSuccess().headers(new HttpHeaders()));

	assertThrowsMatch(LockssRestInvalidResponseException.class,
			  "did not return X-LockssRepo-Artifact-Committed",
			  () -> {repository.isArtifactCommitted("collection1",
								"artifact1");});
        mockServer.verify();
    }

    @Test
    public void testIsArtifactCommitted_true() throws Exception {
        HttpHeaders mockHeaders = new HttpHeaders();
        mockHeaders.add(ArtifactConstants.ARTIFACT_STATE_COMMITTED, "true");

        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1", BASEURL)))
                .andExpect(method(HttpMethod.HEAD))
                .andRespond(withSuccess().headers(mockHeaders));

        Boolean result = repository.isArtifactCommitted("collection1", "artifact1");
        mockServer.verify();
        assertTrue(result);
    }

    @Test
    public void testIsArtifactCommitted_false() throws Exception {
        HttpHeaders mockHeaders = new HttpHeaders();
        mockHeaders.add(ArtifactConstants.ARTIFACT_STATE_COMMITTED, "false");

        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1", BASEURL)))
                .andExpect(method(HttpMethod.HEAD))
                .andRespond(withSuccess().headers(mockHeaders));

        Boolean result = repository.isArtifactCommitted("collection1", "artifact1");
        mockServer.verify();
        assertFalse(result);
    }

    @Test
    public void testGetArtifact_400() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?url=url1&version=latest", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST));

        Artifact result = repository.getArtifact("collection1", "auid1", "url1");
        mockServer.verify();

        assertNull(result);
    }

    @Test
    public void testGetArtifact_404() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?url=url1&version=latest", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Artifact result = repository.getArtifact("collection1", "auid1", "url1");
        mockServer.verify();

        assertNull(result);
    }

    @Test
    public void testGetArtifact_empty() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?url=url1&version=latest", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Artifact result = repository.getArtifact("collection1", "auid1", "url1");
        mockServer.verify();

        assertNull(result);
    }

    @Test
    public void testGetArtifact_found() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?url=url1&version=latest", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[{\"id\":\"1\",\"version\":2}], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Artifact result = repository.getArtifact("collection1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertEquals("1", result.getId());
        assertEquals(2, result.getVersion().intValue());
    }

    @Test
    public void testGetArtifact_failure() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?url=url1&version=latest", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

        Artifact result = repository.getArtifact("collection1", "auid1", "url1");
        mockServer.verify();

        assertNull(result);
    }

    @Test
    public void testAddArtifact_iae() throws Exception {
        try {
            repository.addArtifact(null);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testAddArtifact_success() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts", BASEURL)))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess("{\"id\":\"1\",\"version\":2}", MediaType.APPLICATION_JSON));

        byte buf[] = new byte[] {};

        Artifact result = repository.addArtifact(new ArtifactData(new ArtifactIdentifier("1", "collection1", "auid1", "url1", 2), new HttpHeaders(), new ByteArrayInputStream(buf), new BasicStatusLine(new ProtocolVersion("protocol1", 4, 5), 3, null), "storageUrl1", new RepositoryArtifactMetadata("{\"artifactId\":\"1\",\"committed\":\"true\",\"deleted\":\"false\"}")));
        mockServer.verify();

        assertNotNull(result);
        assertEquals("1", result.getId());
        assertEquals(2, result.getVersion().intValue());
    }

    @Test
    public void testAddArtifact_failure() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts", BASEURL)))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withServerError());

        try {
            repository.addArtifact(new ArtifactData(new ArtifactIdentifier("1", "collection1", "auid1", "url1", 2), new HttpHeaders(), new ByteArrayInputStream(new byte[] {}), new BasicStatusLine(new ProtocolVersion("protocol1", 4, 5), 3, null), "storageUrl1", new RepositoryArtifactMetadata("{\"artifactId\":\"1\",\"committed\":\"true\",\"deleted\":\"false\"}")));
            fail("Should have thrown IOException");
        } catch (IOException ioe) {}
    }

    @Test
    public void testGetArtifactData_failure() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts/artifactid1", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

        try {
            repository.getArtifactData("collection1", "artifactid1");
            fail("Should have thrown IOException");
        } catch (IOException ioe) {}
    }

    @Test
    public void testGetArtifactData_success() throws Exception {
        // Setup reference artifact data headers
        HttpHeaders referenceHeaders = new HttpHeaders();
        referenceHeaders.add("key1", "value1");
        referenceHeaders.add("key2", "value2");

        // Setup reference artifact data
        ArtifactData reference = new ArtifactData(
                new ArtifactIdentifier("artifact1", "collection1", "auid1", "url1", 2),
                referenceHeaders,
                new ByteArrayInputStream("hello world".getBytes()),
                new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"),
                "storageUrl1",
                new RepositoryArtifactMetadata("{\"artifactId\":\"artifact1\",\"committed\":\"true\",\"deleted\":\"false\"}")
        );

        // Convenience variables
        ArtifactIdentifier refId = reference.getIdentifier();
        RepositoryArtifactMetadata refRepoMd = reference.getRepositoryMetadata();

        // Setup mocked artifact data headers
        HttpHeaders transportHeaders = new HttpHeaders();

        transportHeaders.set(ArtifactConstants.ARTIFACT_ID_KEY, refId.getId());
        transportHeaders.set(ArtifactConstants.ARTIFACT_COLLECTION_KEY, refId.getCollection());
        transportHeaders.set(ArtifactConstants.ARTIFACT_AUID_KEY, refId.getAuid());
        transportHeaders.set(ArtifactConstants.ARTIFACT_URI_KEY, refId.getUri());
        transportHeaders.set(ArtifactConstants.ARTIFACT_VERSION_KEY, String.valueOf(refId.getVersion()));

        transportHeaders.set(ArtifactConstants.ARTIFACT_STATE_COMMITTED, String.valueOf(refRepoMd.getCommitted()));
        transportHeaders.set(ArtifactConstants.ARTIFACT_STATE_DELETED, String.valueOf(refRepoMd.getDeleted()));

        transportHeaders.set(ArtifactConstants.ARTIFACT_LENGTH_KEY, String.valueOf(reference.getContentLength()));
        transportHeaders.set(ArtifactConstants.ARTIFACT_DIGEST_KEY, reference.getContentDigest());

        // Setup mocked artifact data response
        mockServer.expect(requestTo(String.format("%s/collections/%s/artifacts/%s", BASEURL, refId.getCollection(), refId.getId())))
                .andExpect(method(HttpMethod.GET))
                .andRespond(
                        withSuccess(
                            IOUtils.toByteArray(ArtifactDataUtil.getHttpResponseStreamFromArtifactData(reference)),
                            MediaType.parseMediaType("application/http; msgtype=response")
                        ).headers(transportHeaders)
                );

        // Fetch artifact data through the code we wish to test
        ArtifactData result = repository.getArtifactData(refId.getCollection(), refId.getId());
        mockServer.verify();

        // Verify the artifact data we got matches the reference
        assertNotNull(result);

        assertEquals(refId.getId(), result.getIdentifier().getId());
        assertEquals(refId.getCollection(), result.getIdentifier().getCollection());
        assertEquals(refId.getAuid(), result.getIdentifier().getAuid());
        assertEquals(refId.getUri(), result.getIdentifier().getUri());
        assertEquals(refId.getVersion(), result.getIdentifier().getVersion());

        assertEquals(refRepoMd.getArtifactId(), result.getRepositoryMetadata().getArtifactId());
        assertEquals(refRepoMd.getCommitted(), result.getRepositoryMetadata().getCommitted());
        assertEquals(refRepoMd.getDeleted(), result.getRepositoryMetadata().getDeleted());

        assertEquals(reference.getContentLength(), result.getContentLength());
        assertEquals(reference.getContentDigest(), result.getContentDigest());

        assertTrue(referenceHeaders.entrySet().containsAll(result.getMetadata().entrySet())
                && result.getMetadata().entrySet().containsAll(referenceHeaders.entrySet()));

        assertEquals(200, result.getHttpStatus().getStatusCode());
    }

    @Test
    public void testCommitArtifact_iae() throws Exception {
        try {
            repository.commitArtifact(null, null);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}

        try {
            repository.commitArtifact("collection1", null);
            fail("Should have thrown IllegalArgumentException");
      } catch (IllegalArgumentException iae) {}

        try {
            repository.commitArtifact(null, "auid1");
            fail("Should have thrown IllegalArgumentException");
      } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testCommitArtifact_success() throws Exception {
        HttpHeaders mockHeaders = new HttpHeaders();
        mockHeaders.add(HttpHeaders.CONTENT_TYPE, MediaType.MULTIPART_FORM_DATA_VALUE);

        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1?committed=true", BASEURL)))
                .andExpect(method(HttpMethod.PUT))
                .andRespond(withSuccess("{\"id\":\"1\",\"version\":2}", MediaType.APPLICATION_JSON).headers(mockHeaders));

        Artifact result = repository.commitArtifact("collection1", "artifact1");
        mockServer.verify();

        assertNotNull(result);
        assertEquals("1", result.getId());
        assertEquals(2, result.getVersion().intValue());
    }

    @Test
    public void testCommitArtifact_failure() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1?committed=true", BASEURL)))
                .andExpect(method(HttpMethod.PUT))
                .andRespond(withServerError());

        try {
            repository.commitArtifact("collection1", "artifact1");
            fail("Should have thrown IOException");
        } catch (IOException ioe) {}
    }

    @Test
    public void testDeleteArtifact_iae() throws Exception {
        try {
            repository.deleteArtifact(null, null);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}

        try {
            repository.deleteArtifact("collection1", null);
            fail("Should have thrown IllegalArgumentException");
      } catch (IllegalArgumentException iae) {}

        try {
            repository.deleteArtifact(null, "auid1");
            fail("Should have thrown IllegalArgumentException");
      } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testDeleteArtifact_success() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1", BASEURL)))
                .andExpect(method(HttpMethod.DELETE))
                .andRespond(withSuccess());

        repository.deleteArtifact("collection1", "artifact1");
        mockServer.verify();
    }

    @Test
    public void testDeleteArtifact_notFound() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1", BASEURL)))
                .andExpect(method(HttpMethod.DELETE))
	  .andRespond(withStatus(HttpStatus.NOT_FOUND));

        try {
	    repository.deleteArtifact("collection1", "artifact1");
            fail("Should have thrown LockssNoSuchArtifactIdException");
      } catch (LockssNoSuchArtifactIdException iae) {}
        mockServer.verify();
    }

    @Test
    public void testDeleteArtifact_failure() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1", BASEURL)))
                .andExpect(method(HttpMethod.DELETE))
                .andRespond(withServerError());

        try {
            repository.deleteArtifact("collection1", "artifact1");
            fail("Should have thrown IOException");
        } catch (IOException ioe) {
	  log.fatal("XXXXXXXXXXXXXXXXX", ioe);
	}
        mockServer.verify();
    }

    @Test
    public void testGetAuIds_empty() throws Exception {
        // Test with empty result
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"auids\":[], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<String> auIds = repository.getAuIds("collection1");
        mockServer.verify();

        assertNotNull(auIds);
        assertFalse(auIds.iterator().hasNext());
    }

    @Test
    public void testGetAuIds_success() throws Exception {
        // Test with valid result
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"auids\":[\"auid1\"], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<String> auIds = repository.getAuIds("collection1");
        mockServer.verify();

        assertNotNull(auIds);
        assertTrue(auIds.iterator().hasNext());
        assertEquals("auid1", auIds.iterator().next());
        assertFalse(auIds.iterator().hasNext());
    }

    @Test
    public void testGetAuIds_failure() throws Exception {
        // Test with server error.
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());


	assertThrowsMatch(LockssUncheckedException.class,
		      "500 Internal Server Error",
		      () -> {repository.getAuIds("collection1");});
        mockServer.verify();
    }

    @Test
    public void testGetAllArtifacts_400() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?version=latest", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST));

	assertThrowsMatch(LockssUncheckedException.class,
		      "400 Bad Request",
		      () -> {repository.getArtifacts("collection1", "auid1");});
        mockServer.verify();
    }

    @Test
    public void testGetAllArtifacts_404() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?version=latest", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Iterable<Artifact> result = repository.getArtifacts("collection1", "auid1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifacts_empty() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?version=latest", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifacts("collection1", "auid1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifacts_found() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?version=latest", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[{\"id\":\"1\",\"version\":2}], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifacts("collection1", "auid1");
        mockServer.verify();

        assertNotNull(result);
        assertTrue(result.iterator().hasNext());
        Artifact artifact = result.iterator().next();
        assertEquals("1", artifact.getId());
        assertEquals(2, artifact.getVersion().intValue());
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifacts_failure() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?version=latest", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

	assertThrowsMatch(LockssUncheckedException.class,
		      "500 Internal Server Error",
		      () -> {repository.getArtifacts("collection1", "auid1");});
        mockServer.verify();
    }

    @Test
    public void testGetAllArtifactsAllVersions_400() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?version=all", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST));

	assertThrowsMatch(LockssUncheckedException.class,
		      "400 Bad Request",
		      () -> {repository.getArtifactsAllVersions("collection1",
								"auid1");});
        mockServer.verify();
    }

    @Test
    public void testGetAllArtifactsAllVersions_404() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?version=all", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Iterable<Artifact> result = repository.getArtifactsAllVersions("collection1", "auid1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsAllVersions_empty() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?version=all", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifactsAllVersions("collection1", "auid1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsAllVersions_found() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?version=all", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[{\"id\":\"1\",\"version\":2}], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifactsAllVersions("collection1", "auid1");
        mockServer.verify();

        assertNotNull(result);
        assertTrue(result.iterator().hasNext());
        Artifact artifact = result.iterator().next();
        assertEquals("1", artifact.getId());
        assertEquals(2, artifact.getVersion().intValue());
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsAllVersions_failure() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?version=all", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

	assertThrowsMatch(LockssUncheckedException.class,
		      "500 Internal Server Error",
		      () -> {repository.getArtifactsAllVersions("collection1",
								"auid1");});
        mockServer.verify();
    }

    @Test
    public void testGetAllArtifactsWithPrefix_400() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?urlPrefix=url1", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST));

	assertThrowsMatch(LockssUncheckedException.class,
		      "400 Bad Request",
		      () -> {repository.getArtifactsWithPrefix("collection1",
							       "auid1",
							       "url1");});
        mockServer.verify();
    }

    @Test
    public void testGetAllArtifactsWithPrefix_404() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?urlPrefix=url1", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Iterable<Artifact> result = repository.getArtifactsWithPrefix("collection1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsWithPrefix_empty() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?urlPrefix=url1", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifactsWithPrefix("collection1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsWithPrefix_found() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?urlPrefix=url1", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[{\"id\":\"1\",\"version\":2}], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifactsWithPrefix("collection1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertTrue(result.iterator().hasNext());
        Artifact artifact = result.iterator().next();
        assertEquals("1", artifact.getId());
        assertEquals(2, artifact.getVersion().intValue());
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsWithPrefix_failure() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?urlPrefix=url1", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

	assertThrowsMatch(LockssUncheckedException.class,
		      "500 Internal Server Error",
		      () -> {repository.getArtifactsWithPrefix("collection1",
							       "auid1",
							       "url1");});
        mockServer.verify();
    }

    @Test
    public void testGetAllArtifactsWithPrefixAllVersions_400() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?version=all&urlPrefix=url1", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST));

	assertThrowsMatch(LockssUncheckedException.class,
			  "400 Bad Request",
		      () -> {repository.getArtifactsWithPrefixAllVersions("collection1", "auid1", "url1");});
        mockServer.verify();
    }

    @Test
    public void testGetAllArtifactsWithPrefixAllVersions_404() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?version=all&urlPrefix=url1", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Iterable<Artifact> result = repository.getArtifactsWithPrefixAllVersions("collection1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsWithPrefixAllVersions_empty() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?version=all&urlPrefix=url1", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifactsWithPrefixAllVersions("collection1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsWithPrefixAllVersions_found() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?version=all&urlPrefix=url1", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[{\"id\":\"1\",\"version\":2}], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifactsWithPrefixAllVersions("collection1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertTrue(result.iterator().hasNext());
        Artifact artifact = result.iterator().next();
        assertEquals("1", artifact.getId());
        assertEquals(2, artifact.getVersion().intValue());
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsWithPrefixAllVersions_failure() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?version=all&urlPrefix=url1", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

	assertThrowsMatch(LockssUncheckedException.class,
		      "500 Internal Server Error",
		      () -> {repository.getArtifactsWithPrefixAllVersions("collection1",
									  "auid1",
									  "url1");});
        mockServer.verify();
    }

    @Test
    public void testGetArtifactAllVersions_400() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?url=url1&version=all", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST));

	assertThrowsMatch(LockssUncheckedException.class,
			  "400 Bad Request",
			  () -> {repository.getArtifactsAllVersions("collection1",
								    "auid1",
								    "url1");});
        mockServer.verify();
    }

    @Test
    public void testGetArtifactAllVersions_404() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?url=url1&version=all", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Iterable<Artifact> result = repository.getArtifactsAllVersions("collection1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetArtifactAllVersions_empty() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?url=url1&version=all", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifactsAllVersions("collection1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetArtifactAllVersions_found() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?url=url1&version=all", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[{\"id\":\"1\",\"version\":2}], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifactsAllVersions("collection1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertTrue(result.iterator().hasNext());
        Artifact artifact = result.iterator().next();
        assertEquals("1", artifact.getId());
        assertEquals(2, artifact.getVersion().intValue());
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetArtifactAllVersions_failure() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?url=url1&version=all", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

	assertThrowsMatch(LockssUncheckedException.class,
			  "500 Internal Server Error",
			  () -> {repository.getArtifactsAllVersions("collection1",
								    "auid1",
								    "url1");});
        mockServer.verify();
    }

    @Test
    public void testGetArtifactVersion_400() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?url=url1&version=123", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST));

        Artifact result = repository.getArtifactVersion("collection1", "auid1", "url1", 123);
        mockServer.verify();

        assertNull(result);
    }

    @Test
    public void testGetArtifactVersion_404() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?url=url1&version=123", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Artifact result = repository.getArtifactVersion("collection1", "auid1", "url1", 123);
        mockServer.verify();

        assertNull(result);
    }

    @Test
    public void testGetArtifactVersion_empty() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?url=url1&version=123", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Artifact result = repository.getArtifactVersion("collection1", "auid1", "url1", 123);
        mockServer.verify();

        assertNull(result);
    }

    @Test
    public void testGetArtifactVersion_found() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?url=url1&version=123", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[{\"id\":\"1\",\"version\":123}], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Artifact result = repository.getArtifactVersion("collection1", "auid1", "url1", 123);
        mockServer.verify();

        assertNotNull(result);
        assertEquals("1", result.getId());
        assertEquals(123, result.getVersion().intValue());
    }

    @Test
    public void testGetArtifactVersion_failure() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/artifacts?url=url1&version=123", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

        Artifact result = repository.getArtifactVersion("collection1", "auid1", "url1", 123);
        mockServer.verify();

        assertNull(result);
    }

    @Test
    public void testAuSize_400() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/size?version=all", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST));

        Long result = repository.auSize("collection1", "auid1");
        mockServer.verify();

        assertNotNull(result);
        assertEquals(0, result.longValue());
    }

    @Test
    public void testAuSize_404() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/size?version=all", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Long result = repository.auSize("collection1", "auid1");
        mockServer.verify();

        assertNotNull(result);
        assertEquals(0, result.longValue());
    }

    @Test
    public void testAuSize_empty() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/size?version=all", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("0", MediaType.APPLICATION_JSON));

        Long result = repository.auSize("collection1", "auid1");
        mockServer.verify();

        assertNotNull(result);
        assertEquals(0, result.longValue());
    }

    @Test
    public void testAuSize_found() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/size?version=all", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("123456", MediaType.APPLICATION_JSON));

        Long result = repository.auSize("collection1", "auid1");
        mockServer.verify();

        assertNotNull(result);
        assertEquals(123456, result.longValue());
    }

    @Test
    public void testAuSize_failure() throws Exception {
        mockServer.expect(requestTo(String.format("%s/collections/collection1/aus/auid1/size?version=all", BASEURL)))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

        Long result = repository.auSize("collection1", "auid1");
        mockServer.verify();

        assertNotNull(result);
        assertEquals(0, result.longValue());
    }
}
