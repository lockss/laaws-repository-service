package org.lockss.laaws.rs.controller;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.runner.RunWith;
import org.lockss.laaws.rs.core.LockssNoSuchArtifactIdException;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.core.RestLockssRepository;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactSpec;
import org.lockss.laaws.rs.util.ArtifactDataFactory;
import org.lockss.log.L4JLogger;
import org.lockss.spring.error.LockssRestServiceException;
import org.lockss.spring.test.SpringLockssTestCase4;
import org.lockss.util.ListUtil;
import org.lockss.util.rest.exception.LockssRestHttpException;
import org.lockss.util.time.TimeBase;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.RestClientResponseException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;

import static org.mockito.Mockito.*;

/**
 * Exercises and tests error communication between a LOCKSS Repository Service and its REST client,
 * {@link RestLockssRepository}.
 * <p>
 * This is done by mocking the {@link LockssRepository} used internally by a LOCKSS repository service (see
 * {@link org.lockss.laaws.rs.impl.CollectionsApiServiceImpl}) running in an embedded servlet. A real
 * {@link RestLockssRepository} client is provided to tests to make requests and allow testing of its behavior. The
 * network traffic between the servlet and client can be observed with Wireshark or similar tool.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("mock-lockss-repository")
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
  protected LockssRepository internalRepo;

//  @Autowired
//  private CollectionsApiServiceImpl service;

  /**
   * Test configuration beans.
   * <p>
   * Provides a mock internal LOCKSS repository for use by the embedded LOCKSS Repository Service.
   */
  @TestConfiguration
  @Profile("mock-lockss-repository")
  static class TestLockssRepositoryConfig {
//    @MockBean
//    CollectionsApiServiceImpl service;

//    @Bean
//    public LockssRepository createInitializedRepository(CollectionsApiServiceImpl service) throws IOException {
//      // Create a mock internal LockssRepository for use by the embedded Repository Service
//      LockssRepository repo = mock(LockssRepository.class);
//      service.repo = repo;
//      return repo;
//    }

    // FIXME: It would be cleaner to use @MockBean but creates a conflict with the
    //  "createInitializedRepository" bean created in LockssRepositoryConfig
    @Bean
    public LockssRepository createInitializedRepository() throws IOException {
      // Create a mock internal LockssRepository for use by the embedded Repository Service
      return mock(LockssRepository.class);
    }

    @Bean
    public ArtifactIndex setArtifactIndex() {
      return null;
    }

    @Bean
    public ArtifactDataStore setArtifactDataStore() {
      return null;
    }
  }

// *********************************************************************************************************************
// * JUNIT
// *********************************************************************************************************************

  @Before
  public void setupRestLockssRepository() throws Exception {
    // Set TimeBase into simulated mode
    TimeBase.setSimulated();

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

  private void assertLockssRestHttpException(Executable exec, String pattern, HttpStatus httpStatus,
                                             LockssRestHttpException.ServerErrorType serverErrorType) {

    try {
      exec.execute();
    } catch (Throwable e) {
      log.info("WOLF3", e);

      // Assert we got a LockssRestHttpException
      assertTrue(e instanceof LockssRestHttpException);

      // Cast Exception to LockssRestHttpException
      LockssRestHttpException lrhe = (LockssRestHttpException) e;

      log.debug("lrhe = {}", lrhe);
      log.debug("lrhe.getMessage() = {}", lrhe.getMessage());
      log.debug("lrhe.getHttpStatusMessage() = {}", lrhe.getHttpStatusMessage());
      log.debug("lrhe.getShortMessage() = {}", lrhe.getShortMessage());
      log.debug("lrhe.getServerErrorMessage() = {}", lrhe.getServerErrorMessage());
      log.debug("lrhe.getLocalizedMessage() = {}", lrhe.getLocalizedMessage());

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

        // Q:
        assertEquals(LockssRestHttpException.ServerErrorType.NONE, lrhe.getServerErrorType());
        // assertEquals(serverErrorType, lrhe.getServerErrorType());
      }
    }
  }

// *********************************************************************************************************************
// * TESTS
// *********************************************************************************************************************

  @Test
  public void testIsReady() throws Exception {
    // Mock the internal repository to be "not ready"
    when(internalRepo.isReady()).thenReturn(false);

    // Assert a 503 Service Unavailable response
    assertLockssRestHttpException(
        (Executable) () -> clientRepo.getCollectionIds(), // FIXME: Implement isReady()?
        "LOCKSS repository is not ready", HttpStatus.SERVICE_UNAVAILABLE,
        LockssRestHttpException.ServerErrorType.APPLICATION_ERROR);
  }

  @Test
  public void testGetCollections() throws Exception {
    // Initialize the internal mock LOCKSS repository
    initInternalLockssRepository();

    //// IOException

    when(internalRepo.getCollectionIds())
        .thenThrow(new IOException("Test error message"));

    assertLockssRestHttpException(
        (Executable) () -> clientRepo.getCollectionIds(),
        "Test error message", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.DATA_ERROR);

    //// Exercise unchecked exception response

    // Reset mock
    reset(internalRepo);
    initInternalLockssRepository();

    when(internalRepo.getCollectionIds())
        .thenThrow(new RuntimeException("surprise!"));

    assertLockssRestHttpException(
        (Executable) () -> clientRepo.getCollectionIds(),
        "surprise!", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.UNSPECIFIED_ERROR);
  }

  @Test
  public void testDeleteArtifact() throws Exception {
    // Initialize the internal mock LOCKSS repository
    initInternalLockssRepository();

    //// LockssNoSuchArtifactIdException
    String artifactId = "artifact";

    doThrow(new LockssNoSuchArtifactIdException("Non-existent artifact ID: " + artifactId))
        .when(internalRepo).deleteArtifact("collection", "artifact");

    assertThrowsMatch(LockssNoSuchArtifactIdException.class, "Could not remove artifact", () ->
        clientRepo.deleteArtifact("collection", "artifact"));

    // Reset mock
    reset(internalRepo);
    initInternalLockssRepository();

    //// IOException
    doThrow(new IOException("Test error message"))
        .when(internalRepo).deleteArtifact("collection", "artifact");

    assertLockssRestHttpException(
        (Executable) () -> clientRepo.deleteArtifact("collection", "artifact"),
        "Test error message", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.APPLICATION_ERROR);
  }

  @Test
  public void testGetArtifact() throws Exception {
    // Initialize the internal mock LOCKSS repository
    initInternalLockssRepository();

    //// LockssNoSuchArtifactIdException
    String artifactId = "artifact";
    Exception e = new LockssNoSuchArtifactIdException("Non-existent artifact ID: " + artifactId);

    when(internalRepo.getArtifactData("collection", "artifact"))
        .thenThrow(e);

    assertThrowsMatch(LockssNoSuchArtifactIdException.class, "Artifact not found", () ->
        clientRepo.getArtifactData("collection", "artifact"));

    // Reset mock
    reset(internalRepo);
    initInternalLockssRepository();

    //// Generic LockssRestServiceException
    when(internalRepo.getArtifactData("collection", "artifact"))
        .thenThrow(new LockssRestServiceException(
            HttpStatus.INTERNAL_SERVER_ERROR, "Test error message", null, "/test/path"));

    assertLockssRestHttpException(
        (Executable) () -> clientRepo.getArtifactData("collection", "artifact"),
        "Test error message", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.UNSPECIFIED_ERROR);

    // Reset mock
    reset(internalRepo);
    initInternalLockssRepository();

    //// IOException
    when(internalRepo.getArtifactData("collection", "artifact"))
        .thenThrow(new IOException("Test error message"));

    assertLockssRestHttpException(
        (Executable) () -> clientRepo.getArtifactData("collection", "artifact"),
        "Test error message", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.DATA_ERROR);
  }

  @Test
  public void testUpdateArtifact() throws Exception {
    initInternalLockssRepository();

    //// TODO: Assert 400 if attempting uncommit an artifact

    // Reset mocks
    reset(internalRepo);
    initInternalLockssRepository();

    //// LockssNoSuchArtifactIdException
    when(internalRepo.commitArtifact("collection", "artifact"))
        .thenThrow(new LockssNoSuchArtifactIdException());

    assertThrowsMatch(LockssNoSuchArtifactIdException.class, "non-existent artifact", () ->
        clientRepo.commitArtifact("collection", "artifact"));

    // Reset mocks
    reset(internalRepo);
    initInternalLockssRepository();

    //// IOException
    when(internalRepo.commitArtifact("collection", "artifact"))
        .thenThrow(new IOException("Test error message"));

    assertLockssRestHttpException(
        (Executable) () -> clientRepo.commitArtifact("collection", "artifact"),
        "Test error message", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.APPLICATION_ERROR);
  }

  @Test
  public void testCreateArtifact() throws Exception {
    // Initialize internal repository
    initInternalLockssRepository();

    // Artifact spec to test with
    ArtifactSpec spec = new ArtifactSpec()
        .setCollectionDate(1234)
        .setUrl("http://example.com/")
        .setContentLength(1234)
        .generateContent();

//    //// Assert 400 response caused by unexpected Content-Type of content part
//    // Q: Is this useful? This should never happen using RestLockssRepository
//
//    // Short-circuit all calls to controller method to throw LRSE with 400 Bad Request
//    doThrow(
//        new LockssRestServiceException("Failed to add artifact")
//            .setServerErrorType(LockssRestHttpException.ServerErrorType.NONE)
//            .setHttpStatus(HttpStatus.BAD_REQUEST)
//            .setParsedRequest("mocked request"))
//        .when(service)
//        .createArtifact(
//            ArgumentMatchers.anyString(),
//            ArgumentMatchers.anyString(),
//            ArgumentMatchers.anyString(),
//            ArgumentMatchers.any(MultipartFile.class),
//            ArgumentMatchers.anyLong()
//        );
//
//    // Assert response
//    assertLockssRestHttpException(
//        (Executable) () -> clientRepo.addArtifact(spec.getArtifactData()),
//        "Bad Request", HttpStatus.BAD_REQUEST,
//        LockssRestHttpException.ServerErrorType.NONE);

    //// Assert 500 response caused by IOException thrown by LockssRepository#addArtifact()

    // Reset mocks
//    reset(service);
//    initInternalLockssRepository();
//
//    // Allow call to real controller method
//    doCallRealMethod().when(service).createArtifact(
//        ArgumentMatchers.anyString(),
//        ArgumentMatchers.anyString(),
//        ArgumentMatchers.anyString(),
//        ArgumentMatchers.any(MultipartFile.class),
//        ArgumentMatchers.anyLong()
//    );

    // Set internal repository to throw IOException when attempting to add an artifact
    when(internalRepo.addArtifact(ArgumentMatchers.any(ArtifactData.class)))
        .thenThrow(new IOException("Test error message"));

    // Assert response
    assertLockssRestHttpException(
        (Executable) () -> clientRepo.addArtifact(spec.getArtifactData()),
        "Test error message", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.DATA_ERROR);
  }

  @Test
  public void testGetArtifacts() throws Exception {
    // TODO
  }

  @Test
  public void testGetArtifactsSize() throws Exception {
    // TODO
  }

  @Test
  public void testGetAus() throws Exception {
    // Initialize the internal repository
    initInternalLockssRepository();

    //// Assert AuidContinuationToken validation exception

    URL testEndpoint =
        new URL(String.format("http://localhost:%d/collections/collectionId/aus?continuationToken=test", port));

    HttpUriRequest request = new HttpGet(testEndpoint.toURI());

    assertLockssRestHttpException(
        (Executable) () -> processRequest(request),
        "Invalid continuation token", HttpStatus.BAD_REQUEST,
        LockssRestHttpException.ServerErrorType.NONE);

    // Reset mock
    reset(internalRepo);
    initInternalLockssRepository();

    //// Assert IOException
    when(internalRepo.getAuIds("collectionId"))
        .thenThrow(new IOException("Test error message"));

    log.info("WOLF1");

    // Assert response
    assertLockssRestHttpException(
        (Executable) () -> clientRepo.getAuIds("collectionId"),
        "collection does not exist", HttpStatus.NOT_FOUND,
        LockssRestHttpException.ServerErrorType.UNSPECIFIED_ERROR);

    log.info("WOLF2");

    when(internalRepo.getCollectionIds())
        .thenReturn(ListUtil.list("collectionId"));

    // Assert response
    assertLockssRestHttpException(
        (Executable) () -> clientRepo.getAuIds("collectionId"),
        "Test error message", HttpStatus.INTERNAL_SERVER_ERROR,
        LockssRestHttpException.ServerErrorType.UNSPECIFIED_ERROR);
  }

  @Test
  public void isolatedTest() throws Exception {
    // Initialize internal repository
    initInternalLockssRepository();

    //// Setup mock - throw IOException
    when(internalRepo.getAuIds("collectionId"))
        .thenThrow(new IOException("Test error message"));

    // Assert 404 Not Found response
    assertLockssRestHttpException(
        (Executable) () -> clientRepo.getAuIds("collectionId"),
        "collection does not exist", HttpStatus.NOT_FOUND,
        LockssRestHttpException.ServerErrorType.UNSPECIFIED_ERROR);
  }

  private void processRequest(HttpUriRequest request) throws IOException {
    HttpClientBuilder builder = HttpClientBuilder.create();
    HttpClient client = builder.build();

    HttpResponse response = client.execute(request);
    BufferedInputStream contentStream = new BufferedInputStream(response.getEntity().getContent());

    // Debugging
    contentStream.mark(0);
    log.info("content = {}", IOUtils.toString(contentStream, Charset.defaultCharset()));
    contentStream.reset();

    StatusLine statusLine = response.getStatusLine();

    throw LockssRestHttpException.fromRestClientResponseException(
        new RestClientResponseException(
            "message",
            statusLine.getStatusCode(),
            statusLine.getReasonPhrase(),
            ArtifactDataFactory.transformHeaderArrayToHttpHeaders(response.getAllHeaders()),
            IOUtils.toByteArray(contentStream),
            Charset.defaultCharset()
            ),
        clientRepo.getRestTemplate().getMessageConverters());
  }

//  @Test
//  public void test() throws Exception {
//    assertNotNull(service);
//
//    initInternalLockssRepository();
//
//    String errorMessage = "Could not enumerate collection IDs";
//    String parsedRequest = "test";
//    Exception e = new IOException();
//
//    // Mock controller method
////    when(controller.getCollections()).thenThrow(
////        new LockssRestServiceException(
////            LockssRestHttpException.ServerErrorType.DATA_ERROR,
////            HttpStatus.INTERNAL_SERVER_ERROR,
////            errorMessage, e, parsedRequest)
////    );
//
//
//    when(service.getCollections()).thenThrow(
//        // Translate to LockssRestServiceException and throw
//        new LockssRestServiceException("Artifact not found", e)
//            .setUtcTimestamp(LocalDateTime.now(ZoneOffset.UTC))
//            .setHttpStatus(HttpStatus.NOT_FOUND)
//            .setServletPath("/collections/collection")
//            .setServerErrorType(LockssRestHttpException.ServerErrorType.DATA_ERROR)
//            .setParsedRequest(parsedRequest)
//    );
//
//    assertLockssRestHttpException(
//        (Executable) () -> clientRepo.getCollectionIds(),
//        "Not Found", HttpStatus.NOT_FOUND,
//        LockssRestHttpException.ServerErrorType.DATA_ERROR);
//
//  }
}
