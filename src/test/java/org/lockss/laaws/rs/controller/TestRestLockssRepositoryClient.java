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

import org.apache.http.ProtocolVersion;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.lockss.laaws.rs.core.LockssNoSuchArtifactIdException;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.core.RestLockssRepository;
import org.lockss.laaws.rs.impl.CollectionsApiServiceImpl;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.ArtifactRepositoryState;
import org.lockss.laaws.rs.util.ArtifactDataUtil;
import org.lockss.log.L4JLogger;
import org.lockss.util.LockssUncheckedException;
import org.lockss.util.rest.RestUtil;
import org.lockss.util.rest.exception.LockssRestHttpException;
import org.lockss.util.rest.exception.LockssRestInvalidResponseException;
import org.lockss.spring.test.SpringLockssTestCase4;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.mock.http.MockHttpOutputMessage;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.*;

/**
 * Test of the REST repository client.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class TestRestLockssRepositoryClient extends SpringLockssTestCase4 {
    private final static L4JLogger log = L4JLogger.getLogger();
    private final static String BASEURL = "http://localhost:24610";
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

        // ******************************
        // Reference artifact and headers
        // ******************************

        // Setup reference artifact data headers
        HttpHeaders referenceHeaders = new HttpHeaders();
        referenceHeaders.add("key1", "value1");
        referenceHeaders.add("key1", "value2");
        referenceHeaders.add("key2", "value3");

	String testData = "hello world";

        // Setup reference artifact data
        ArtifactData reference = new ArtifactData(
            new ArtifactIdentifier("artifact1", "collection1", "auid1", "url1", 2),
            referenceHeaders,
            new ByteArrayInputStream(testData.getBytes()),
            new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"),
            new URI("storageUrl1"),
            null
        );

        reference.setContentDigest("test");
        reference.setContentLength(testData.length());

        // Multipart response parts
        MultiValueMap<String, Object> parts = CollectionsApiServiceImpl.generateMultipartResponseFromArtifactData(
            reference, LockssRepository.IncludeContent.IF_SMALL, 4096L
        );

        // ****************************
        // Setup mocked server response
        // ****************************

        // Convert multipart response and its parts to a byte array
        FormHttpMessageConverter converter = new FormHttpMessageConverter();
        converter.setPartConverters(RestUtil.getRestTemplate().getMessageConverters());
        MockHttpOutputMessage outputMessage = new MockHttpOutputMessage();
        converter.write(parts, MediaType.MULTIPART_FORM_DATA, outputMessage);

        // Setup mocked server response
        mockServer
            .expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1?includeContent=IF_SMALL", BASEURL)))
            .andExpect(method(HttpMethod.GET))
            .andRespond(
                withSuccess(outputMessage.getBodyAsBytes(), outputMessage.getHeaders().getContentType())
            );

	assertThrowsMatch(LockssRestInvalidResponseException.class,
			  "Missing artifact repository state",
			  () -> {repository.isArtifactCommitted("collection1",
								"artifact1");});
        mockServer.verify();
    }

    @Test
    public void testIsArtifactCommitted_true() throws Exception {

        // ******************************
        // Reference artifact and headers
        // ******************************

        // Setup reference artifact data headers
        HttpHeaders referenceHeaders = new HttpHeaders();
        referenceHeaders.add("key1", "value1");
        referenceHeaders.add("key1", "value2");
        referenceHeaders.add("key2", "value3");

	String testData = "hello world";

        // Setup reference artifact data
        ArtifactData reference = new ArtifactData(
            new ArtifactIdentifier("artifact1", "collection1", "auid1", "url1", 2),
            referenceHeaders,
            new ByteArrayInputStream(testData.getBytes()),
            new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"),
            new URI("storageUrl1"),
            new ArtifactRepositoryState("{\"artifactId\":\"artifact1\",\"committed\":\"true\",\"deleted\":\"false\"}")
        );

        reference.setContentDigest("test");
        reference.setContentLength(testData.length());

        // Multipart response parts
        MultiValueMap<String, Object> parts = CollectionsApiServiceImpl.generateMultipartResponseFromArtifactData(
            reference, LockssRepository.IncludeContent.IF_SMALL, 4096L
        );

        // ****************************
        // Setup mocked server response
        // ****************************

        // Convert multipart response and its parts to a byte array
        FormHttpMessageConverter converter = new FormHttpMessageConverter();
        converter.setPartConverters(RestUtil.getRestTemplate().getMessageConverters());
        MockHttpOutputMessage outputMessage = new MockHttpOutputMessage();
        converter.write(parts, MediaType.MULTIPART_FORM_DATA, outputMessage);

        // Setup mocked server response
        mockServer
            .expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1?includeContent=IF_SMALL", BASEURL)))
            .andExpect(method(HttpMethod.GET))
            .andRespond(
                withSuccess(outputMessage.getBodyAsBytes(), outputMessage.getHeaders().getContentType())
            );

        Boolean result = repository.isArtifactCommitted("collection1", "artifact1");
        mockServer.verify();
        assertTrue(result);
    }

    @Test
    public void testIsArtifactCommitted_false() throws Exception {

        // ******************************
        // Reference artifact and headers
        // ******************************

        // Setup reference artifact data headers
        HttpHeaders referenceHeaders = new HttpHeaders();
        referenceHeaders.add("key1", "value1");
        referenceHeaders.add("key1", "value2");
        referenceHeaders.add("key2", "value3");

	String testData = "hello world";

        // Setup reference artifact data
        ArtifactData reference = new ArtifactData(
            new ArtifactIdentifier("artifact1", "collection1", "auid1", "url1", 2),
            referenceHeaders,
            new ByteArrayInputStream(testData.getBytes()),
            new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"),
            new URI("storageUrl1"),
            new ArtifactRepositoryState("{\"artifactId\":\"artifact1\",\"committed\":\"false\"," +
                "\"deleted\":\"false\"}")
        );

        reference.setContentDigest("test");
        reference.setContentLength(testData.length());

        // Multipart response parts
        MultiValueMap<String, Object> parts = CollectionsApiServiceImpl.generateMultipartResponseFromArtifactData(
            reference, LockssRepository.IncludeContent.IF_SMALL, 4096L
        );

        // ****************************
        // Setup mocked server response
        // ****************************

        // Convert multipart response and its parts to a byte array
        FormHttpMessageConverter converter = new FormHttpMessageConverter();
        converter.setPartConverters(RestUtil.getRestTemplate().getMessageConverters());
        MockHttpOutputMessage outputMessage = new MockHttpOutputMessage();
        converter.write(parts, MediaType.MULTIPART_FORM_DATA, outputMessage);

        // Setup mocked server response
        mockServer
            .expect(requestTo(String.format("%s/collections/collection1/artifacts/artifact1?includeContent=IF_SMALL", BASEURL)))
            .andExpect(method(HttpMethod.GET))
            .andRespond(
                withSuccess(outputMessage.getBodyAsBytes(), outputMessage.getHeaders().getContentType())
            );

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

        byte buf[] = new byte[]{};

        Artifact result = repository.addArtifact(
            new ArtifactData(
                new ArtifactIdentifier("1", "collection1", "auid1", "url1", 2),
                new HttpHeaders(),
                new ByteArrayInputStream(buf),
                new BasicStatusLine(
                    new ProtocolVersion("protocol1", 4, 5), 3, null),
                new URI("storageUrl1"),
                new ArtifactRepositoryState("{\"artifactId\":\"1\",\"committed\":\"true\",\"deleted\":\"false\"}")
            )
        );

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
            repository.addArtifact(
                new ArtifactData(
                    new ArtifactIdentifier("1", "collection1", "auid1", "url1", 2),
                    new HttpHeaders(),
                    new ByteArrayInputStream(new byte[]{}),
                    new BasicStatusLine(new ProtocolVersion("protocol1", 4, 5), 3, null),
                    new URI("storageUrl1"),
                    new ArtifactRepositoryState("{\"artifactId\":\"1\",\"committed\":\"true\",\"deleted\":\"false\"}")
                )
            );

            fail("Should have thrown IOException");
        } catch (IOException ioe) {}
    }

    @Test
    public void testGetArtifactData_failure() throws Exception {
        mockServer
            .expect(requestTo(
                String.format("%s/collections/collection1/artifacts/artifactid1?includeContent=ALWAYS", BASEURL))
            )
            .andExpect(method(HttpMethod.GET))
            .andRespond(withServerError());

        try {
            repository.getArtifactData("collection1", "artifactid1");
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
                                       boolean isWebCrawl, Boolean isSmall) throws Exception {

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

        BasicStatusLine httpStatus = !isWebCrawl ?
            null : new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK");

        // Setup reference artifact data
        ArtifactData reference = new ArtifactData(
            new ArtifactIdentifier("artifact", "collection", "auid", "url", 1),
            referenceHeaders,
            new ByteArrayInputStream(content.getBytes()),
            httpStatus,
            new URI("storageUrl1"),
            new ArtifactRepositoryState("{\"artifactId\":\"artifact\",\"committed\":\"true\",\"deleted\":\"false\"}")
        );

        reference.setContentLength(content.length());
        reference.setContentDigest("some made-up hash");

        // Convenience variables
        ArtifactIdentifier refId = reference.getIdentifier();
        ArtifactRepositoryState refRepoMd = reference.getArtifactRepositoryState();

        // Artifact is small if its size is less than or equal to the threshold
        long includeContentMaxSize = (isSmall == null || isSmall == true) ? content.length() : content.length() - 1;

        // Multipart response parts
        MultiValueMap<String, Object> parts = CollectionsApiServiceImpl.generateMultipartResponseFromArtifactData(
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
        builder.path(String.format("/collections/%s/artifacts/%s", refId.getCollection(), refId.getId()));
        builder.queryParam("includeContent", includeContent);

        // Setup mocked server response
        mockServer
            .expect(requestTo(builder.toUriString()))
            // FIXME: Why doesn't this work?
//             .andExpect(queryParam("includeContent", String.valueOf(includeContent)))
            .andExpect(method(HttpMethod.GET))
            .andRespond(withSuccess(outputMessage.getBodyAsBytes(), outputMessage.getHeaders().getContentType()));

        // ************************************
        // Verify result from getArtifactData()
        // ************************************

        // Fetch artifact data
        ArtifactData result = repository.getArtifactData(refId.getCollection(), refId.getId(), includeContent);
        assertNotNull(result);

        // Verify we made the expected REST API call
        mockServer.verify();

        // Verify artifact repository properties
        assertNotNull(result.getIdentifier());
        assertEquals(refId.getId(), result.getIdentifier().getId());
        assertEquals(refId.getCollection(), result.getIdentifier().getCollection());
        assertEquals(refId.getAuid(), result.getIdentifier().getAuid());
        assertEquals(refId.getUri(), result.getIdentifier().getUri());
        assertEquals(refId.getVersion(), result.getIdentifier().getVersion());

        // Verify artifact repository state
        assertNotNull(result.getArtifactRepositoryState());
        assertEquals(refRepoMd.getArtifactId(), result.getArtifactRepositoryState().getArtifactId());
        assertEquals(refRepoMd.getCommitted(), result.getArtifactRepositoryState().getCommitted());
        assertEquals(refRepoMd.getDeleted(), result.getArtifactRepositoryState().getDeleted());

        // Verify misc. artifact properties
        assertEquals(reference.getContentLength(), result.getContentLength());
        assertEquals(reference.getContentDigest(), result.getContentDigest());

        // Verify artifact header set equality
        assertTrue(referenceHeaders.entrySet().containsAll(result.getMetadata().entrySet())
            && result.getMetadata().entrySet().containsAll(referenceHeaders.entrySet()));

        // Verify artifact HTTP status is present if expected
        if (isWebCrawl) {
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
	  log.debug("Logging trace of IOException", ioe);
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
