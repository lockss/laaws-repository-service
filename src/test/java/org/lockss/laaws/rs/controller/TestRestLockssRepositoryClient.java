/*
 * Copyright (c) 2019-2020, Board of Trustees of Leland Stanford Jr. University,
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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.ProtocolVersion;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.runner.RunWith;
import org.lockss.log.L4JLogger;
import org.lockss.spring.test.SpringLockssTestCase4;
import org.lockss.util.LockssUncheckedException;
import org.lockss.util.rest.RestUtil;
import org.lockss.util.rest.exception.LockssRestHttpException;
import org.lockss.util.rest.repo.LockssNoSuchArtifactIdException;
import org.lockss.util.rest.repo.LockssRepository;
import org.lockss.util.rest.repo.RestLockssRepository;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactData;
import org.lockss.util.rest.repo.model.ArtifactIdentifier;
import org.lockss.util.rest.repo.model.AuSize;
import org.lockss.util.rest.repo.util.ArtifactDataUtil;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.mock.http.MockHttpOutputMessage;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.client.RequestMatcher;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.response.MockRestResponseCreators.*;

/**
 * Test of the REST repository client.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class TestRestLockssRepositoryClient extends SpringLockssTestCase4 {
    private final static L4JLogger log = L4JLogger.getLogger();
    private final static String BASEURL = "http://localhost:24610";
    private final static ObjectMapper mapper = new ObjectMapper();
    protected RestLockssRepository repository;
    protected MockRestServiceServer mockServer;

    /**
     * Creates the REST repository client to be used in the test.
     * @throws Exception if there are problems.
     */
    @Before
    public void makeLockssRepository() throws Exception {
      // The authentication credentials.
      String userName = null;
      String password = null;

      // Must create the RestLockssRepository before the
      // MockRestServiceServer, as the latter replaces the RequestFactory
      // in the RestTemplate with its own mock RequestFactory
      repository = new RestLockssRepository(new URL(BASEURL), userName, password);

      mockServer = MockRestServiceServer.createServer(repository.getRestTemplate());
    }

    @Test
    public void testGetNamespaces_empty() throws Exception {
        URI endpoint = new URI(BASEURL + "/namespaces");

        // Test with empty result
        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("[]", MediaType.APPLICATION_JSON));

        Iterable<String> namespaces = repository.getNamespaces();
        mockServer.verify();

        assertNotNull(namespaces);
        assertFalse(namespaces.iterator().hasNext());
    }

    @Test
    public void testGetNamespaces_success() throws Exception {
        URI endpoint = new URI(BASEURL + "/namespaces");

        // Test with valid result
        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("[\"ns1\"]", MediaType.APPLICATION_JSON));

        Iterable<String> namespaces = repository.getNamespaces();
        mockServer.verify();

        assertNotNull(namespaces);
        assertTrue(namespaces.iterator().hasNext());
        assertEquals("ns1", namespaces.iterator().next());
        assertFalse(namespaces.iterator().hasNext());
    }

    @Test
    public void testGetNamespaces_failure() throws Exception {
        URI endpoint = new URI(BASEURL + "/namespaces");

        // Test with server error.
        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

	assertThrowsMatch(LockssRestHttpException.class,
		      "500",
		      () -> {repository.getNamespaces();});
        mockServer.verify();
    }

    @Test
    public void testGetArtifact_400() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&url=url1&version=latest");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST));

        Artifact result = repository.getArtifact("ns1", "auid1", "url1");
        mockServer.verify();

        assertNull(result);
    }

    @Test
    public void testGetArtifact_404() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&url=url1&version=latest");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Artifact result = repository.getArtifact("ns1", "auid1", "url1");
        mockServer.verify();

        assertNull(result);
    }

    @Test
    public void testGetArtifact_empty() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&url=url1&version=latest");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Artifact result = repository.getArtifact("ns1", "auid1", "url1");
        mockServer.verify();

        assertNull(result);
    }

    @Test
    public void testGetArtifact_found() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&url=url1&version=latest");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[{\"uuid\":\"1\",\"version\":2}], \"pageInfo\":{}}",
                    MediaType.APPLICATION_JSON));

        Artifact result = repository.getArtifact("ns1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertEquals("1", result.getUuid());
        assertEquals(2, result.getVersion().intValue());
    }

    @Test
    public void testGetArtifact_failure() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&url=url1&version=latest");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

        Artifact result = repository.getArtifact("ns1", "auid1", "url1");
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
        URI endpoint = new URI(BASEURL + "/artifacts");

        mockServer.expect(uriRequestTo(endpoint))
            .andExpect(method(HttpMethod.POST))
            .andRespond(withSuccess("{\"uuid\":\"1\",\"version\":2}", MediaType.APPLICATION_JSON));

        byte buf[] = new byte[]{};

        String js1 = "{\"artifactUuid\": \"test\", \"entryDate\": 1234, \"artifactState\": \"COPIED\"}";

        Artifact result = repository.addArtifact(
            new ArtifactData(
                new ArtifactIdentifier("1", "ns1", "auid1", "url1", 2),
                new HttpHeaders(),
                new ByteArrayInputStream(buf),
                new BasicStatusLine(
                    new ProtocolVersion("protocol1", 4, 5), 3, null),
                new URI("storageUrl1")));

        mockServer.verify();

        assertNotNull(result);
        assertEquals("1", result.getUuid());
        assertEquals(2, result.getVersion().intValue());
    }

    @Test
    public void testAddArtifact_failure() throws Exception {
        URI endpoint = new URI(BASEURL + "/artifacts");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withServerError());

        String js1 = "{\"artifactId\":\"1\",\"entryDate\":0,\"committed\":\"true\",\"deleted\":\"false\"}";

        try {
            repository.addArtifact(
                new ArtifactData(
                    new ArtifactIdentifier("1", "ns1", "auid1", "url1", 2),
                    new HttpHeaders(),
                    new ByteArrayInputStream(new byte[]{}),
                    new BasicStatusLine(new ProtocolVersion("protocol1", 4, 5), 3, null),
                    new URI("storageUrl1")));

            fail("Should have thrown IOException");
        } catch (IOException ioe) {}
    }

    @Test
    public void testGetArtifactData_failure() throws Exception {
        URI endpoint = new URI(BASEURL + "/artifacts/artifactid1?namespace=ns1&includeContent=ALWAYS");

        mockServer
            .expect(uriRequestTo(endpoint))
            .andExpect(method(HttpMethod.GET))
            .andRespond(withServerError());

        try {
            repository.getArtifactData("ns1", "artifactid1");
            fail("Should have thrown IOException");
        } catch (IOException ioe) {}
    }

    /**
     * Test for {@link RestLockssRepository#getArtifactData(String, String, LockssRepository.IncludeContent)}.
     *
     * @throws Exception
     */
    @Test
    public void testGetArtifactData() throws Exception {
      // Artifact data
      runTestGetArtifactData(LockssRepository.IncludeContent.NEVER, false, true);
      runTestGetArtifactData(LockssRepository.IncludeContent.NEVER, false, false);
      runTestGetArtifactData(LockssRepository.IncludeContent.IF_SMALL, false, true);
      runTestGetArtifactData(LockssRepository.IncludeContent.IF_SMALL, false, false);
      runTestGetArtifactData(LockssRepository.IncludeContent.ALWAYS, false, true);
      runTestGetArtifactData(LockssRepository.IncludeContent.ALWAYS, false, false);

      // Artifact data contains a web crawl
      runTestGetArtifactData(LockssRepository.IncludeContent.NEVER, true, true);
      runTestGetArtifactData(LockssRepository.IncludeContent.NEVER, true, false);
      runTestGetArtifactData(LockssRepository.IncludeContent.IF_SMALL, true, true);
      runTestGetArtifactData(LockssRepository.IncludeContent.IF_SMALL, true, false);
      runTestGetArtifactData(LockssRepository.IncludeContent.ALWAYS, true, true);
      runTestGetArtifactData(LockssRepository.IncludeContent.ALWAYS, true, false);
    }

    public void runTestGetArtifactData(LockssRepository.IncludeContent includeContent,
                                       boolean isHttpResponse, Boolean isSmall) throws Exception {

        // ******************************
        // Reference artifact and headers
        // ******************************

        // Setup reference artifact data headers
        HttpHeaders referenceHeaders = new HttpHeaders();
        referenceHeaders.add("key1", "value1");
        referenceHeaders.add("key1", "value2");
        referenceHeaders.add("key2", "value3");

        String content = "\"Dog watched his human cry, concerned. Where was human's smile? Probably lost somewhere, " +
            "dog thought. That was OK. Dog knew how to fetch.\"";

        BasicStatusLine httpStatus = !isHttpResponse ?
            null : new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK");

        String js1 = "{\"artifactUuid\": \"artifact\", \"entryDate\": 1234, \"artifactState\": \"COPIED\"}";

        // Setup reference artifact data
        ArtifactData reference = new ArtifactData(
            new ArtifactIdentifier("artifact", "namespace", "auid", "url", 1),
            referenceHeaders,
            new ByteArrayInputStream(content.getBytes()),
            httpStatus,
            new URI("storageUrl1"));

        reference.setContentLength(content.length());
        reference.setContentDigest("some made-up hash");

        // Convenience variables
        ArtifactIdentifier refId = reference.getIdentifier();

        // Artifact is small if its size is less than or equal to the threshold
        long includeContentMaxSize = (isSmall == null || isSmall == true) ? content.length() : content.length() - 1;

        // Multipart response parts
        MultiValueMap<String, Object> parts = ArtifactDataUtil.generateMultipartMapFromArtifactData(
            reference, includeContent, includeContentMaxSize
        );

        // ****************************
        // Setup mocked server response
        // ****************************

        // Convert multipart response and its parts to a byte array
        FormHttpMessageConverter converter = new FormHttpMessageConverter();
        converter.setPartConverters(RestUtil.getRestTemplate().getMessageConverters());
        MockHttpOutputMessage outputMessage = new MockHttpOutputMessage();
        converter.write(parts, MediaType.MULTIPART_FORM_DATA, outputMessage);

        // Build expected endpoint
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(BASEURL);
        builder.path(String.format("/artifacts/%s", refId.getUuid()));
        builder.queryParam("namespace", refId.getNamespace());
        builder.queryParam("includeContent", includeContent);

        // Setup mocked server response
        mockServer
            .expect(uriRequestTo(builder.build().toUri()))
            // FIXME: Why doesn't this work?
//             .andExpect(queryParam("includeContent", String.valueOf(includeContent)))
            .andExpect(method(HttpMethod.GET))
            .andRespond(withSuccess(outputMessage.getBodyAsBytes(), outputMessage.getHeaders().getContentType()));

        // ************************************
        // Verify result from getArtifactData()
        // ************************************

        // Fetch artifact data
        ArtifactData result = repository.getArtifactData(refId.getNamespace(), refId.getUuid(), includeContent);
        assertNotNull(result);

        // Verify we made the expected REST API call
        mockServer.verify();

        // Verify artifact repository properties
        assertNotNull(result.getIdentifier());
        assertEquals(refId.getUuid(), result.getIdentifier().getUuid());
        assertEquals(refId.getNamespace(), result.getIdentifier().getNamespace());
        assertEquals(refId.getAuid(), result.getIdentifier().getAuid());
        assertEquals(refId.getUri(), result.getIdentifier().getUri());
        assertEquals(refId.getVersion(), result.getIdentifier().getVersion());

        // Verify misc. artifact properties
        assertEquals(reference.getContentLength(), result.getContentLength());
        assertEquals(reference.getContentDigest(), result.getContentDigest());

        // Verify artifact HTTP status is present if expected
        if (isHttpResponse) {
            // Verify artifact header equality (only HTTP response artifacts)
            assertTrue(referenceHeaders.entrySet().containsAll(result.getHttpHeaders().entrySet())
                && result.getHttpHeaders().entrySet().containsAll(referenceHeaders.entrySet()));

            // Assert artifact HTTP response status matches
            assertArrayEquals(
                ArtifactDataUtil.getHttpStatusByteArray(httpStatus),
                ArtifactDataUtil.getHttpStatusByteArray(result.getHttpStatus())
            );
        } else {
            // Assert artifact has no HTTP response status
            assertNull(result.getHttpStatus());
        }

        // Verify artifact content is present if expected
        if ((includeContent == LockssRepository.IncludeContent.IF_SMALL && isSmall) ||
            (includeContent == LockssRepository.IncludeContent.ALWAYS)) {

            // Assert artifact data has content and that its InputStream has the expected content
            assertTrue(result.hasContentInputStream());
            InputStream inputStream = result.getInputStream();
            assertNotNull(inputStream);
            assertInputStreamMatchesString(content, inputStream);

        } else {
            // Assert artifact has no content
            assertFalse(result.hasContentInputStream());
        }

        // Clean-up after ourselves
        mockServer.reset();
    }

    @Test
    public void testCommitArtifact_iae() throws Exception {
        try {
            repository.commitArtifact(null, null);
            fail("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException iae) {}

        try {
            repository.commitArtifact("ns1", null);
            fail("Should have thrown IllegalArgumentException");
      } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testCommitArtifact_success() throws Exception {
        HttpHeaders mockHeaders = new HttpHeaders();
        mockHeaders.add(HttpHeaders.CONTENT_TYPE, MediaType.MULTIPART_FORM_DATA_VALUE);

        URI endpoint = new URI(BASEURL + "/artifacts/artifact1?namespace=ns1&committed=true");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.PUT))
                .andRespond(withSuccess("{\"uuid\":\"1\",\"version\":2}", MediaType.APPLICATION_JSON).headers(mockHeaders));

        Artifact result = repository.commitArtifact("ns1", "artifact1");
        mockServer.verify();

        assertNotNull(result);
        assertEquals("1", result.getUuid());
        assertEquals(2, result.getVersion().intValue());
    }

    @Test
    public void testCommitArtifact_failure() throws Exception {
        URI endpoint = new URI(BASEURL + "/artifacts/artifact1?namespace=ns1&committed=true");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.PUT))
                .andRespond(withServerError());

        try {
            repository.commitArtifact("ns1", "artifact1");
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
            repository.deleteArtifact("ns1", null);
            fail("Should have thrown IllegalArgumentException");
      } catch (IllegalArgumentException iae) {}
    }

    @Test
    public void testDeleteArtifact_success() throws Exception {
        URI endpoint = new URI(BASEURL + "/artifacts/artifact1?namespace=ns1");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.DELETE))
                .andRespond(withSuccess());

        repository.deleteArtifact("ns1", "artifact1");
        mockServer.verify();
    }

    @Test
    public void testDeleteArtifact_notFound() throws Exception {
        URI endpoint = new URI(BASEURL + "/artifacts/artifact1?namespace=ns1");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.DELETE))
	  .andRespond(withStatus(HttpStatus.NOT_FOUND));

        try {
	    repository.deleteArtifact("ns1", "artifact1");
            fail("Should have thrown LockssNoSuchArtifactIdException");
      } catch (LockssNoSuchArtifactIdException iae) {}
        mockServer.verify();
    }

    @Test
    public void testDeleteArtifact_failure() throws Exception {
        URI endpoint = new URI(BASEURL + "/artifacts/artifact1?namespace=ns1");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.DELETE))
                .andRespond(withServerError());

        try {
            repository.deleteArtifact("ns1", "artifact1");
            fail("Should have thrown IOException");
        } catch (IOException ioe) {
	  log.debug("Logging trace of IOException", ioe);
	}
        mockServer.verify();
    }

    @Test
    public void testGetAuIds_empty() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus?namespace=ns1");

        // Test with empty result
        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"auids\":[], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<String> auIds = repository.getAuIds("ns1");
        mockServer.verify();

        assertNotNull(auIds);
        assertFalse(auIds.iterator().hasNext());
    }

    @Test
    public void testGetAuIds_success() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus?namespace=ns1");

        // Test with valid result
        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"auids\":[\"auid1\"], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<String> auIds = repository.getAuIds("ns1");
        mockServer.verify();

        assertNotNull(auIds);
        assertTrue(auIds.iterator().hasNext());
        assertEquals("auid1", auIds.iterator().next());
        assertFalse(auIds.iterator().hasNext());
    }

    @Test
    public void testGetAuIds_failure() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus?namespace=ns1");

        // Test with server error.
        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());


	assertThrowsMatch(LockssRestHttpException.class,
		      "500 Internal Server Error",
		      () -> {repository.getAuIds("ns1");});
        mockServer.verify();
    }

    @Test
    public void testGetAllArtifacts_400() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&version=latest");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST));

	assertThrowsMatch(LockssUncheckedException.class,
		      "400 Bad Request",
		      () -> {repository.getArtifacts("ns1", "auid1");});
        mockServer.verify();
    }

    @Test
    public void testGetAllArtifacts_404() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&version=latest");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Iterable<Artifact> result = repository.getArtifacts("ns1", "auid1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifacts_empty() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&version=latest");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifacts("ns1", "auid1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifacts_found() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&version=latest");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[{\"uuid\":\"1\",\"version\":2}], \"pageInfo\":{}}",
                    MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifacts("ns1", "auid1");
        mockServer.verify();

        assertNotNull(result);
        assertTrue(result.iterator().hasNext());
        Artifact artifact = result.iterator().next();
        assertEquals("1", artifact.getUuid());
        assertEquals(2, artifact.getVersion().intValue());
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifacts_failure() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&version=latest");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

	assertThrowsMatch(LockssUncheckedException.class,
		      "500 Internal Server Error",
		      () -> {repository.getArtifacts("ns1", "auid1");});
        mockServer.verify();
    }

    @Test
    public void testGetAllArtifactsAllVersions_400() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&version=all");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST));

	assertThrowsMatch(LockssUncheckedException.class,
		      "400 Bad Request",
		      () -> {repository.getArtifactsAllVersions("ns1",
								"auid1");});
        mockServer.verify();
    }

    @Test
    public void testGetAllArtifactsAllVersions_404() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&version=all");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Iterable<Artifact> result = repository.getArtifactsAllVersions("ns1", "auid1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsAllVersions_empty() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&version=all");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifactsAllVersions("ns1", "auid1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsAllVersions_found() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&version=all");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[{\"uuid\":\"1\",\"version\":2}], \"pageInfo\":{}}",
                    MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifactsAllVersions("ns1", "auid1");
        mockServer.verify();

        assertNotNull(result);
        assertTrue(result.iterator().hasNext());
        Artifact artifact = result.iterator().next();
        assertEquals("1", artifact.getUuid());
        assertEquals(2, artifact.getVersion().intValue());
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsAllVersions_failure() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&version=all");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

	assertThrowsMatch(LockssUncheckedException.class,
		      "500 Internal Server Error",
		      () -> {repository.getArtifactsAllVersions("ns1",
								"auid1");});
        mockServer.verify();
    }

    @Test
    public void testGetAllArtifactsWithPrefix_400() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&urlPrefix=url1");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST));

	assertThrowsMatch(LockssUncheckedException.class,
		      "400 Bad Request",
		      () -> {repository.getArtifactsWithPrefix("ns1",
							       "auid1",
							       "url1");});
        mockServer.verify();
    }

    @Test
    public void testGetAllArtifactsWithPrefix_404() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&urlPrefix=url1");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Iterable<Artifact> result = repository.getArtifactsWithPrefix("ns1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsWithPrefix_empty() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&urlPrefix=url1");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifactsWithPrefix("ns1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsWithPrefix_found() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&urlPrefix=url1");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[{\"uuid\":\"1\",\"version\":2}], \"pageInfo\":{}}",
                    MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifactsWithPrefix("ns1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertTrue(result.iterator().hasNext());
        Artifact artifact = result.iterator().next();
        assertEquals("1", artifact.getUuid());
        assertEquals(2, artifact.getVersion().intValue());
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsWithPrefix_failure() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&urlPrefix=url1");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

	assertThrowsMatch(LockssUncheckedException.class,
		      "500 Internal Server Error",
		      () -> {repository.getArtifactsWithPrefix("ns1",
							       "auid1",
							       "url1");});
        mockServer.verify();
    }

    @Test
    public void testGetAllArtifactsWithPrefixAllVersions_400() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&version=all&urlPrefix=url1");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST));

	assertThrowsMatch(LockssUncheckedException.class,
			  "400 Bad Request",
		      () -> {repository.getArtifactsWithPrefixAllVersions("ns1", "auid1", "url1");});
        mockServer.verify();
    }

    @Test
    public void testGetAllArtifactsWithPrefixAllVersions_404() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&version=all&urlPrefix=url1");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Iterable<Artifact> result = repository.getArtifactsWithPrefixAllVersions("ns1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsWithPrefixAllVersions_empty() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&version=all&urlPrefix=url1");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifactsWithPrefixAllVersions("ns1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsWithPrefixAllVersions_found() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&version=all&urlPrefix=url1");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[{\"uuid\":\"1\",\"version\":2}], \"pageInfo\":{}}",
                    MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifactsWithPrefixAllVersions("ns1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertTrue(result.iterator().hasNext());
        Artifact artifact = result.iterator().next();
        assertEquals("1", artifact.getUuid());
        assertEquals(2, artifact.getVersion().intValue());
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetAllArtifactsWithPrefixAllVersions_failure() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&version=all&urlPrefix=url1");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

	assertThrowsMatch(LockssUncheckedException.class,
		      "500 Internal Server Error",
		      () -> {repository.getArtifactsWithPrefixAllVersions("ns1",
									  "auid1",
									  "url1");});
        mockServer.verify();
    }

    @Test
    public void testGetArtifactAllVersions_400() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&url=url1&version=all");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST));

	assertThrowsMatch(LockssUncheckedException.class,
			  "400 Bad Request",
			  () -> {repository.getArtifactsAllVersions("ns1",
								    "auid1",
								    "url1");});
        mockServer.verify();
    }

    @Test
    public void testGetArtifactAllVersions_404() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&url=url1&version=all");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Iterable<Artifact> result = repository.getArtifactsAllVersions("ns1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetArtifactAllVersions_empty() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&url=url1&version=all");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifactsAllVersions("ns1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetArtifactAllVersions_found() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&url=url1&version=all");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[{\"uuid\":\"1\",\"version\":2}], \"pageInfo\":{}}",
                    MediaType.APPLICATION_JSON));

        Iterable<Artifact> result = repository.getArtifactsAllVersions("ns1", "auid1", "url1");
        mockServer.verify();

        assertNotNull(result);
        assertTrue(result.iterator().hasNext());
        Artifact artifact = result.iterator().next();
        assertEquals("1", artifact.getUuid());
        assertEquals(2, artifact.getVersion().intValue());
        assertFalse(result.iterator().hasNext());
    }

    @Test
    public void testGetArtifactAllVersions_failure() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&url=url1&version=all");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

	assertThrowsMatch(LockssUncheckedException.class,
			  "500 Internal Server Error",
			  () -> {repository.getArtifactsAllVersions("ns1",
								    "auid1",
								    "url1");});
        mockServer.verify();
    }

    /**
     * Implementation of {@link RequestMatcher} that canonicalizes the expected and actual URIs using
     * {@link UriComponentsBuilder} for comparison. Allows for the arbitrary ordering of query arguments.
     * This should not be used where the order of query arguments is sensitive and must match.
     */
    private static class UriRequestMatcher implements RequestMatcher {
        private URI expectedUri;

        public UriRequestMatcher(URI expectedUri) {
            this.expectedUri = expectedUri;
        }

        @Override
        public void match(ClientHttpRequest clientHttpRequest) throws IOException, AssertionError {
            UriComponents expected = UriComponentsBuilder.fromUri(expectedUri).build();
            UriComponents actual = UriComponentsBuilder.fromUri(clientHttpRequest.getURI()).build();
            assertEquals(expected, actual);
        }
    }

    /**
     * Creates an instance of {@link UriRequestMatcher} for an expected URI.
     * @param expectedUri A {@link URI} containing the expected URI.
     * @return A {@link UriRequestMatcher} for the expected URI.
     */
    private static RequestMatcher uriRequestTo(URI expectedUri) {
        return new UriRequestMatcher(expectedUri);
    }

    @Test
    public void testGetArtifactVersion_400() throws Exception {
        URI uri = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&url=url1&version=123");

        mockServer
            .expect(uriRequestTo(uri))
//            .andExpect(queryParam("namespace", "ns1"))
//            .andExpect(queryParam("url", "url1"))
//            .andExpect(queryParam("version", "123"))
            .andExpect(method(HttpMethod.GET))
            .andRespond(withStatus(HttpStatus.BAD_REQUEST));

        Artifact result = repository.getArtifactVersion("ns1", "auid1", "url1", 123);
        mockServer.verify();

        assertNull(result);
    }

    @Test
    public void testGetArtifactVersion_404() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&url=url1&version=123");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        Artifact result = repository.getArtifactVersion("ns1", "auid1", "url1", 123);
        mockServer.verify();

        assertNull(result);
    }

    @Test
    public void testGetArtifactVersion_empty() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&url=url1&version=123");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[], \"pageInfo\":{}}", MediaType.APPLICATION_JSON));

        Artifact result = repository.getArtifactVersion("ns1", "auid1", "url1", 123);
        mockServer.verify();

        assertNull(result);
    }

    @Test
    public void testGetArtifactVersion_found() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&url=url1&version=123");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"artifacts\":[{\"uuid\":\"1\",\"version\":123}], \"pageInfo\":{}}",
                    MediaType.APPLICATION_JSON));

        Artifact result = repository.getArtifactVersion("ns1", "auid1", "url1", 123);
        mockServer.verify();

        assertNotNull(result);
        assertEquals("1", result.getUuid());
        assertEquals(123, result.getVersion().intValue());
    }

    @Test
    public void testGetArtifactVersion_failure() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/artifacts?namespace=ns1&url=url1&version=123");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

        Artifact result = repository.getArtifactVersion("ns1", "auid1", "url1", 123);
        mockServer.verify();

        assertNull(result);
    }

    @Test
    public void testAuSize_400() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/size?namespace=ns1&version=all");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST));

        assertThrows(LockssRestHttpException.class,
                     () -> repository.auSize("ns1", "auid1"));

        mockServer.verify();
    }

    @Test
    public void testAuSize_404() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/size?namespace=ns1&version=all");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(HttpStatus.NOT_FOUND));

        assertThrows(LockssRestHttpException.class,
                     () -> repository.auSize("ns1", "auid1"));

        mockServer.verify();
    }

    @Test
    public void testAuSize_empty() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/size?namespace=ns1&version=all");

        mockServer.expect(uriRequestTo(endpoint))
            .andExpect(method(HttpMethod.GET))
            .andRespond(withSuccess("{\"totalAllVersions\":\"0\",\"totalLatestVersions\":\"0\",\"totalWarcSize\":\"0\"}", MediaType.APPLICATION_JSON));

        AuSize auSize = repository.auSize("ns1", "auid1");
        mockServer.verify();

        assertNotNull(auSize);
        assertEquals(0L, (long)auSize.getTotalAllVersions());
        assertEquals(0L, (long)auSize.getTotalLatestVersions());
        assertEquals(0L, (long)auSize.getTotalWarcSize());
    }

    @Test
    public void testAuSize_found() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/size?namespace=ns1&version=all");

        mockServer.expect(uriRequestTo(endpoint))
            .andExpect(method(HttpMethod.GET))
            .andRespond(withSuccess("{\"totalAllVersions\":\"12345\",\"totalLatestVersions\":\"54321\",\"totalWarcSize\":\"67890\"}", MediaType.APPLICATION_JSON));

        AuSize auSize = repository.auSize("ns1", "auid1");
        mockServer.verify();

        assertNotNull(auSize);
        assertEquals(12345L, (long)auSize.getTotalAllVersions());
        assertEquals(54321L, (long)auSize.getTotalLatestVersions());
        assertEquals(67890L, (long)auSize.getTotalWarcSize());
    }

    @Test
    public void testAuSize_failure() throws Exception {
        URI endpoint = new URI(BASEURL + "/aus/auid1/size?namespace=ns1&version=all");

        mockServer.expect(uriRequestTo(endpoint))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withServerError());

        assertThrows(LockssRestHttpException.class,
                     () -> repository.auSize("ns1", "auid1"));

        mockServer.verify();
    }
}
