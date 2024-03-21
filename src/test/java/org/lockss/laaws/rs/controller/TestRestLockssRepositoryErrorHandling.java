package org.lockss.laaws.rs.controller;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.runner.RunWith;
import org.lockss.laaws.rs.RepositoryApplication;
import org.lockss.log.L4JLogger;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.spring.error.LockssRestServiceException;
import org.lockss.spring.test.SpringLockssTestCase4;
import org.lockss.util.ListUtil;
import org.lockss.util.rest.exception.LockssRestHttpException;
import org.lockss.util.rest.repo.LockssNoSuchArtifactIdException;
import org.lockss.util.rest.repo.LockssRepository;
import org.lockss.util.rest.repo.RestLockssRepository;
import org.lockss.util.rest.repo.util.ArtifactDataUtil;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.time.TimeBase;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.RestClientResponseException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collections;

import static org.mockito.Mockito.*;

/**
 * Exercises and tests error communication between a LOCKSS Repository Service and its REST client,
 * {@link RestLockssRepository}.
 * <p>
 * This is done by mocking the {@link LockssRepository} used internally by a LOCKSS Repository Service (running in an
 * embedded servlet). A real {@link RestLockssRepository} client is provided to tests to make requests and allow testing
 * of its behavior. The network traffic between the servlet and client can be observed with Wireshark or similar tool.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(classes = { MyTestConfig.class, RepositoryApplication.class })
public class TestRestLockssRepositoryErrorHandling extends SpringLockssTestCase4 {
  private final static L4JLogger log = L4JLogger.getLogger();

  /**
   * Random TCP port assigned to the embedded servlet by the Spring Boot test environment.
   */
  @LocalServerPort
  private int port;

  /**
   * Client {@link RestLockssRepository} to the embedded LOCKSS Repository Service servlet.
   */
  protected RestLockssRepository clientRepo;

  /**
   * Internal LOCKSS repository used by the embedded LOCKSS Repository Service.
   */
  @Autowired
  protected BaseLockssRepository internalRepo;

//  /**
//   * Test configuration beans.
//   * <p>
//   * Provides a mock internal LOCKSS repository for use by the embedded LOCKSS Repository Service.
//   */
//  @TestConfiguration
//  @Primary
//  private static class TestLockssRepositoryConfig {
//    public TestLockssRepositoryConfig() { }
//    // NOTE: It would be cleaner to use @MockBean but creates a conflict with the
//    // "createInitializedRepository" bean created in LockssRepositoryConfig
//    @Bean
//    public LockssRepository createInitializedRepository() throws IOException {
//      // Create a mock internal LockssRepository for use by the embedded Repository Service
//      return mock(LockssRepository.class);
//    }
//
//    @Bean
//    public ArtifactIndex setArtifactIndex() {
//      return null;
//    }
//
//    @Bean
//    public ArtifactDataStore setArtifactDataStore() {
//      return null;
//    }
//  }

// *********************************************************************************************************************
// * JUNIT
// *********************************************************************************************************************

  @Before
  public void setupRestLockssRepository() throws Exception {
    // Set TimeBase into simulated mode
    TimeBase.setSimulated();

//    LockssRepository mockedRepo = mock(LockssRepository.class);
//    ReflectionTestUtils.setField(internalRepo, "createLockssRepository", mockedRepo);

    // Reset internal LockssRepository mock behavior
    reset(internalRepo);

    // Create a new RestLockssRepository client to the test server
    this.clientRepo = makeRestLockssRepositoryClient();
  }

  @After
  public void teardownRestLockssRepository() throws Exception {
    this.clientRepo = null;
  }

// *********************************************************************************************************************
// * TEST UTILITIES
// *********************************************************************************************************************

  /**
   * Initializes the internal mock LOCKSS repository.
   */
  public void initInternalLockssRepository() {
    when(internalRepo.isReady()).thenReturn(true);
  }

  /**
   * Returns a {@link RestLockssRepository} REST client to the
   *
   * @return a LockssRepository with the newly built LOCKSS repository.
   * @throws Exception if there are problems.
   */
  public RestLockssRepository makeRestLockssRepositoryClient() throws Exception {
    URL testEndpoint = new URL(String.format("http://localhost:%d", port));

    log.info("testEndpoint = {}", testEndpoint);

    return new RestLockssRepository(testEndpoint, null, null);
  }

  /**
   * Asserts an {@link Executable} throws a {@link LockssRestHttpException} and its details.
   *
   * @param exec            An {@link Executable}
   * @param pattern         A {@link String} containing the expected server error message.
   * @param httpStatus      A {@link HttpStatus} containing the expected HTTP status.
   * @param serverErrorType A {@link LockssRestHttpException.ServerErrorType} containing the expected server error type.
   */
  private void assertLockssRestHttpException(Executable exec, String pattern, HttpStatus httpStatus,
                                             LockssRestHttpException.ServerErrorType serverErrorType) {

    try {
      exec.execute();
      fail("Expected LockssRestHttpException to be thrown");
    } catch (AssertionError e) {
      throw e;
    } catch (Throwable e) {
      // Assert we got a LockssRestHttpException
      assertTrue(e instanceof LockssRestHttpException);

      // Cast Exception to LockssRestHttpException
      LockssRestHttpException lrhe = (LockssRestHttpException) e;

      log.debug("Caught a LockssRestHttpException", lrhe);

      // Assert HTTP status message
      assertNotNull(lrhe.getHttpStatusMessage());
      assertMatchesRE(pattern, lrhe.getServerErrorMessage());
//      assertMatchesRE(pattern, lrhe.getServerErrorMessage());

      // Assert HTTP status
      assertEquals(httpStatus, lrhe.getHttpStatus());

      // Assert server error type if 5xx series error
      if (lrhe.getHttpStatus().is5xxServerError()) {
        assertEquals(serverErrorType, lrhe.getServerErrorType());
      } else {
        // Assert a 4xx series error and not a server error
        assertTrue(lrhe.getHttpStatus().is4xxClientError());
        assertEquals(LockssRestHttpException.ServerErrorType.NONE, lrhe.getServerErrorType());
      }
    }
  }

  /**
   * Executes an Apache Commons {@link HttpRequest} and throws a {@link LockssRestHttpException} with details
   * parsed from the error response message.
   *
   * @param request A subclass of {@link HttpRequest} to execute.
   * @throws IOException Thrown if an IO error occurs or a {@link LockssRestHttpException} with details from the
   *                     error response.
   * @return Returns a {@link HttpResponse} if the response is not a 4xx or 5xx series error.
   */
  private HttpResponse processRequest(HttpUriRequest request) throws IOException {
    // Create an HTTP client
    HttpClientBuilder builder = HttpClientBuilder.create();
    HttpClient client = builder.build();

    // Use HTTP client to execute request
    HttpResponse response = client.execute(request);

    // Get the HTTP response body stream
    BufferedInputStream responseBody = new BufferedInputStream(response.getEntity().getContent());

    // Debugging
    responseBody.mark(0);
    log.info("content = {}", IOUtils.toString(responseBody, Charset.defaultCharset()));
    responseBody.reset();

    // Get HTTP response status
    StatusLine statusLine = response.getStatusLine();
    HttpStatus status = HttpStatus.valueOf(statusLine.getStatusCode());

    // Throw LRHE if we got an error response
    if (status.is4xxClientError() || status.is5xxServerError()) {
      throw LockssRestHttpException.fromRestClientResponseException(
          new RestClientResponseException(
              "message",
              statusLine.getStatusCode(),
              statusLine.getReasonPhrase(),
              ArtifactDataUtil.transformHeaderArrayToHttpHeaders(response.getAllHeaders()),
              IOUtils.toByteArray(responseBody),
              Charset.defaultCharset()
          ),
          clientRepo.getRestTemplate().getMessageConverters());
    }

    // Return HttpResponse for further processing
    return response;
  }

// *********************************************************************************************************************
// * TESTS
// *********************************************************************************************************************

  @Test
  public void testIsReady() throws Exception {
    // Mock the internal repository to be "not ready"
    when(internalRepo.isReady()).thenReturn(false);

    //// Assert a 503 Service Unavailable response
    assertLockssRestHttpException(
        (Executable) () -> clientRepo.getNamespaces(), // FIXME: Implement isReady()?
        "LOCKSS repository is not ready", HttpStatus.SERVICE_UNAVAILABLE,
        LockssRestHttpException.ServerErrorType.APPLICATION_ERROR);
  }

  @Test
  public void testGetNamespaces() throws Exception {
    // Initialize the internal mock LOCKSS repository
    initInternalLockssRepository();

    //// Assert 500 Internal Server Error if IOException is thrown
    doThrow(new IOException("IOException from mock"))
        .when(internalRepo).getNamespaces();


    assertLockssRestHttpException(
        (Executable) () -> clientRepo.getNamespaces(),
        "IOException from mock", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.DATA_ERROR);

    // Reset mock
    reset(internalRepo);
    initInternalLockssRepository();

    //// Assert unspecified 500 Internal Server Error if generic RuntimeException is thrown
    doThrow(new RuntimeException("RuntimeException from mock"))
        .when(internalRepo).getNamespaces();

    assertLockssRestHttpException(
        (Executable) () -> clientRepo.getNamespaces(),
        "RuntimeException from mock", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.UNSPECIFIED_ERROR);
  }

  @Test
  public void testDeleteArtifact() throws Exception {
    // Initialize the internal mock LOCKSS repository
    initInternalLockssRepository();

    //// LockssNoSuchArtifactIdException
    String artifactId = "artifact";

    doThrow(new LockssNoSuchArtifactIdException("Non-existent artifact ID: " + artifactId))
        .when(internalRepo).deleteArtifact("namespace", "artifact");

    assertThrowsMatch(LockssNoSuchArtifactIdException.class, "Could not remove artifact", () ->
        clientRepo.deleteArtifact("namespace", "artifact"));

    // Reset mock
    reset(internalRepo);
    initInternalLockssRepository();

    //// Assert 500 Internal Server Error if IOException is thrown
    doThrow(new IOException("Test error message"))
        .when(internalRepo).deleteArtifact("namespace", "artifact");

    assertLockssRestHttpException(
        (Executable) () -> clientRepo.deleteArtifact("namespace", "artifact"),
        "Test error message", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.APPLICATION_ERROR);
  }

  @Test
  public void testGetArtifact() throws Exception {
    ArtifactSpec spec = new ArtifactSpec()
        .setNamespace("ns1")
        .setArtifactUuid("artifactid1")
        .setUrl("test")
        .setContentLength(1)
        .setContentDigest("test")
        .setCollectionDate(0);

    // Initialize the internal mock LOCKSS repository
    initInternalLockssRepository();

    //// LockssNoSuchArtifactIdException
    Exception e = new LockssNoSuchArtifactIdException("Non-existent artifact ID: " + spec.getArtifactUuid());

    doThrow(e).when(internalRepo).getArtifactData(spec.getNamespace(), spec.getArtifactUuid());

    assertThrowsMatch(LockssNoSuchArtifactIdException.class, "Artifact not found",
        () -> clientRepo.getArtifactData(spec.getArtifact(), LockssRepository.IncludeContent.ALWAYS));

    // Reset mock
    reset(internalRepo);
    initInternalLockssRepository();

    //// Generic LockssRestServiceException
    doThrow(new LockssRestServiceException(HttpStatus.INTERNAL_SERVER_ERROR, "Test error message", null, "/test/path"))
        .when(internalRepo).getArtifactData(spec.getNamespace(), spec.getArtifactUuid());

    assertLockssRestHttpException(
        () -> clientRepo.getArtifactData(spec.getArtifact(), LockssRepository.IncludeContent.ALWAYS),
        "Test error message", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.UNSPECIFIED_ERROR);

    // Reset mock
    reset(internalRepo);
    initInternalLockssRepository();

    //// IOException
    doThrow(new IOException("Test error message"))
        .when(internalRepo).getArtifactData(spec.getNamespace(), spec.getArtifactUuid());

    assertLockssRestHttpException(
        () -> clientRepo.getArtifactData(spec.getArtifact(), LockssRepository.IncludeContent.ALWAYS),
        "Test error message", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.DATA_ERROR);
  }

  @Test
  public void testUpdateArtifact() throws Exception {
    // Initialize internal LOCKSS repository
    initInternalLockssRepository();

    //// Assert 400 Bad request if attempting uncommit an artifact
    URL testEndpoint =
        new URL(
            String.format("http://localhost:%d/artifacts/artifactId?committed=false&namespace=namespace1", port));

    // Create a PUT request
    HttpPut request = new HttpPut(testEndpoint.toURI());

    HttpEntity entity = MultipartEntityBuilder.create()
        .build();

    request.addHeader(entity.getContentType());
    request.addHeader(entity.getContentEncoding());

    request.setEntity(entity);

    // Process request and assert response
    assertLockssRestHttpException(
        (Executable) () -> processRequest(request),
        "Cannot uncommit", HttpStatus.BAD_REQUEST,
        LockssRestHttpException.ServerErrorType.NONE);

    // Reset mocks
    reset(internalRepo);
    initInternalLockssRepository();

    //// Assert 404 if LockssNoSuchArtifactIdException is thrown
    doThrow(new LockssNoSuchArtifactIdException())
        .when(internalRepo).commitArtifact("namespace", "artifact");

    assertThrowsMatch(LockssNoSuchArtifactIdException.class, "non-existent artifact", () ->
        clientRepo.commitArtifact("namespace", "artifact"));

    // Reset mocks
    reset(internalRepo);
    initInternalLockssRepository();

    //// Assert 500 Internal Server Error if IOException is thrown
    doThrow(new IOException("Test error message"))
        .when(internalRepo).commitArtifact("namespace", "artifact");

    assertLockssRestHttpException(
        (Executable) () -> clientRepo.commitArtifact("namespace", "artifact"),
        "Test error message", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.APPLICATION_ERROR);
  }

  @Test
  public void testCreateArtifact() throws Exception {
    // Initialize internal repository
    initInternalLockssRepository();

    //// Assert 400 Bad Request if invalid URI is provided

    // Artifact spec with bad URI
    ArtifactSpec spec1 = new ArtifactSpec()
        .setCollectionDate(1234)
        .setUrl("")
        .setContentLength(1234)
        .generateContent();

    // Assert response
    assertLockssRestHttpException(
        (Executable) () -> clientRepo.addArtifact(spec1.getArtifactData()),
        "URI has not been provided", HttpStatus.BAD_REQUEST,
        LockssRestHttpException.ServerErrorType.NONE);

    //// Assert 500 Internal Server error if an IOException occurs

    // Artifact spec to test with
    ArtifactSpec spec2 = new ArtifactSpec()
        .setCollectionDate(1234)
        .setUrl("http://example.com/")
        .setContentLength(1234)
        .generateContent();

    // Set internal repository to throw IOException when attempting to add an artifact
    doThrow(new IOException("IOException from mock"))
        .when(internalRepo).addArtifact(ArgumentMatchers.any());

    // Assert response
    assertLockssRestHttpException(
        (Executable) () -> clientRepo.addArtifact(spec2.getArtifactData()),
        "IOException from mock", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.DATA_ERROR);
  }

  @Test
  public void testGetArtifacts() throws Exception {
    // Initialize the internal LOCKSS repository
    initInternalLockssRepository();

    //// Assert invalid paging limit results in a 400
    URL endpoint1 =
        new URL(String.format("http://localhost:%d/aus/auid/artifacts?limit=-1&namespace=namespace1", port));

    HttpUriRequest request1 = new HttpGet(endpoint1.toURI());

    assertLockssRestHttpException(
        (Executable) () -> processRequest(request1),
        "must be a positive integer", HttpStatus.BAD_REQUEST,
        LockssRestHttpException.ServerErrorType.NONE);

    //// Assert 400 Bad Request if invalid continuation token
    URL endpoint2 =
        new URL(String.format("http://localhost:%d/aus/auid/artifacts?continuationToken=test&namespace=namespace1",
            port));

    HttpUriRequest request2 = new HttpGet(endpoint2.toURI());

    assertLockssRestHttpException(
        (Executable) () -> processRequest(request2),
        "Invalid continuation token", HttpStatus.BAD_REQUEST,
        LockssRestHttpException.ServerErrorType.NONE);

    //// Assert 400 Bad request if urlPrefix and url provided (should be mutually exclusive)
    URL endpoint3 =
        new URL(String.format("http://localhost:%d/aus/auid/artifacts?urlPrefix=a&url=b&namespace=namespace1", port));

    // Create a GET request
    HttpUriRequest request3 = new HttpGet(endpoint3.toURI());

    // Process request and assert response
    assertLockssRestHttpException(
        (Executable) () -> processRequest(request3),
        "arguments are mutually exclusive", HttpStatus.BAD_REQUEST,
        LockssRestHttpException.ServerErrorType.NONE);

    //// Assert 400 Bad request if version specified without url or urlPrefix
    URL endpoint4 =
        new URL(String.format("http://localhost:%d/aus/auid/artifacts?version=1&namespace=namespace1", port));

    // Create a GET request
    HttpUriRequest request4 = new HttpGet(endpoint4.toURI());

    // Process request and assert response
    assertLockssRestHttpException(
        (Executable) () -> processRequest(request4),
        "'version' argument requires a 'url' argument", HttpStatus.BAD_REQUEST,
        LockssRestHttpException.ServerErrorType.NONE);

    //// Assert 400 Bad request if includeCommitted without url or urlPrefix
    URL endpoint5 = new URL(String.format(
        "http://localhost:%d/aus/auid/artifacts?includeUncommitted=true&namespace=namespace1", port));

    // Create a GET request
    HttpUriRequest request5 = new HttpGet(endpoint5.toURI());

    // Process request and assert response
    assertLockssRestHttpException(
        (Executable) () -> processRequest(request5),
        "Including an uncommitted artifact requires a specific 'version' argument", HttpStatus.BAD_REQUEST,
        LockssRestHttpException.ServerErrorType.NONE);

    //// Assert 400 Bad request if negative version number
    URL endpoint6 = new URL(String.format(
        "http://localhost:%d/aus/auid/artifacts?version=-1&url=test&namespace=namespace1", port));

    // Create a GET request
    HttpUriRequest request6 = new HttpGet(endpoint6.toURI());

    // Process request and assert response
    assertLockssRestHttpException(
        (Executable) () -> processRequest(request6),
        "The 'version' argument is not a positive integer", HttpStatus.BAD_REQUEST,
        LockssRestHttpException.ServerErrorType.NONE);

    //// Assert 400 Bad request if invalid version number
    URL endpoint7 = new URL(String.format(
        "http://localhost:%d/aus/auid/artifacts?version=NaN&url=test&namespace=namespace1", port));

    // Create a GET request
    HttpUriRequest request7 = new HttpGet(endpoint7.toURI());

    // Process request and assert response
    assertLockssRestHttpException(
        (Executable) () -> processRequest(request7),
        "The 'version' argument is invalid", HttpStatus.BAD_REQUEST,
        LockssRestHttpException.ServerErrorType.NONE);

    //// Assert invalid namespace
//     URL endpoint8 = new URL(String.format(
//         "http://localhost:%d/aus/auid/artifacts?namespace=namespace1", port));

//     // Create a GET request
//     HttpUriRequest request8 = new HttpGet(endpoint8.toURI());

//     // Setup mock
//     when(internalRepo.getNamespaces()).thenReturn(Collections.emptyList());

//     // Process request and assert response
//     assertLockssRestHttpException(
//         (Executable) () -> processRequest(request8),
//         "namespace does not exist", HttpStatus.NOT_FOUND,
//         LockssRestHttpException.ServerErrorType.NONE);

    //// Assert invalid AUID
//    URL endpoint6 =
//        new URL(
//            String.format("http://localhost:%d/aus/auid/size", port));
//
//    // Create a GET request
//    HttpUriRequest request6 = new HttpGet(endpoint6.toURI());
//
//    // Setup mock
//    when(internalRepo.getNamespaces()).thenReturn(ListUtil.list("namespace"));
//    when(internalRepo.getAuIds(ArgumentMatchers.anyString())).thenReturn(Collections.emptyList());
//
//    // Process request and assert response
//    assertLockssRestHttpException(
//        (Executable) () -> processRequest(request6),
//        "archival unit has no artifacts", HttpStatus.NOT_FOUND,
//        LockssRestHttpException.ServerErrorType.DATA_ERROR);

    // Reset mock
    reset(internalRepo);
    initInternalLockssRepository();

    //// TODO: Is there a combination of query parameters that causes the endpoint to not "understand" the request?

    //// Assert 500 Internal Server Error if IOException is thrown
    URL endpoint9 = new URL(String.format(
        "http://localhost:%d/aus/auid/artifacts?version=all&namespace=namespace1", port));

    // Create a GET request
    HttpUriRequest request9 = new HttpGet(endpoint9.toURI());

    // Setup mock
    when(internalRepo.getNamespaces()).thenReturn(ListUtil.list("namespace1"));
    when(internalRepo.getArtifactsAllVersions("namespace1", "auid"))
        .thenThrow(new IOException("Test error message"));

    // Process request and assert response
    assertLockssRestHttpException(
        (Executable) () -> processRequest(request9),
        "Test error message", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.DATA_ERROR);
  }

  @Test
  public void testGetArtifactsSize() throws Exception {
    // Initialize the internal LOCKSS repository
    initInternalLockssRepository();

    //// Assert invalid namespace
    URL endpoint =
        new URL(
            String.format("http://localhost:%d/aus/auid/size?namespace=namespace1", port));

    // Create a GET request
    HttpUriRequest request = new HttpGet(endpoint.toURI());

    // Setup mock
    when(internalRepo.getNamespaces()).thenReturn(Collections.emptyList());

    // Rest mock
    reset(internalRepo);
    initInternalLockssRepository();

    //// Assert 500 Internal Server Error if IOException is thrown
    when(internalRepo.getNamespaces())
        .thenReturn(ListUtil.list("namespace"));
    when(internalRepo.auSize("namespace", "auid"))
        .thenThrow(new IOException("Test error message"));

    // Process request and assert response
    assertLockssRestHttpException(
        (Executable) () -> clientRepo.auSize("namespace", "auid"),
        "Test error message", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.APPLICATION_ERROR);
  }

  @Test
  public void testGetAus() throws Exception {
    // Initialize the internal repository
    initInternalLockssRepository();

    //// Assert invalid paging limit results in a 400
    URL endpoint1 = new URL(String.format("http://localhost:%d/aus?limit=-1&namespace=namespace1", port));

    HttpUriRequest request1 = new HttpGet(endpoint1.toURI());

    assertLockssRestHttpException(
        (Executable) () -> processRequest(request1),
        "must be a positive integer", HttpStatus.BAD_REQUEST,
        LockssRestHttpException.ServerErrorType.NONE);

    //// Assert invalid AuidContinuationToken results in a 400
    URL endpoint2 =
        new URL(String.format("http://localhost:%d/aus?continuationToken=test&namespace=namespace1", port));

    HttpUriRequest request = new HttpGet(endpoint2.toURI());

    assertLockssRestHttpException(
        (Executable) () -> processRequest(request),
        "Invalid continuation token", HttpStatus.BAD_REQUEST,
        LockssRestHttpException.ServerErrorType.NONE);

    // Reset mock
    reset(internalRepo);
    initInternalLockssRepository();

    //// Assert IOException
    when(internalRepo.getNamespaces())
        .thenReturn(ListUtil.list("namespace1"));
    when(internalRepo.getAuIds("namespace1"))
        .thenThrow(new IOException("Test error message"));

    // Assert response
    assertLockssRestHttpException(
        (Executable) () -> clientRepo.getAuIds("namespace1"),
        "Test error message", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.UNSPECIFIED_ERROR);
  }
}
