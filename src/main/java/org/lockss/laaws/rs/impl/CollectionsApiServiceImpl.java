/*
 * Copyright (c) 2017-2019, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.laaws.rs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.IterableUtils;
import org.lockss.config.Configuration;
import org.lockss.laaws.rs.api.CollectionsApiDelegate;
import org.lockss.laaws.rs.core.ArtifactCache;
import org.lockss.laaws.rs.core.LockssNoSuchArtifactIdException;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.core.RestLockssRepository;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.*;
import org.lockss.log.L4JLogger;
import org.lockss.spring.base.BaseSpringApiServiceImpl;
import org.lockss.spring.base.LockssConfigurableService;
import org.lockss.spring.error.LockssRestServiceException;
import org.lockss.util.TimerQueue;
import org.lockss.util.UrlUtil;
import org.lockss.util.jms.JmsUtil;
import org.lockss.util.rest.exception.LockssRestHttpException;
import org.lockss.util.time.Deadline;
import org.lockss.util.time.TimeUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.*;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.multipart.MultipartFile;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.StreamSupport;

/**
 * Service for accessing the repository artifacts.
 */
@Service
public class CollectionsApiServiceImpl
    extends BaseSpringApiServiceImpl
    implements CollectionsApiDelegate, LockssConfigurableService {

  private static L4JLogger log = L4JLogger.getLogger();
  private static final String APPLICATION_HTTP_RESPONSE_VALUE =
      "application/http;msgtype=response";
  private static final MediaType APPLICATION_HTTP_RESPONSE =
      MediaType.parseMediaType(APPLICATION_HTTP_RESPONSE_VALUE);

  public static final String PREFIX = "org.lockss.repository.";

  // Config params for response pagination sizes.

  /**
   * Default number of Artifacts that will be returned in a single (paged)
   * response
   */
  public static final String PARAM_DEFAULT_ARTIFACT_PAGESIZE =
      PREFIX + "artifact.pagesize.default";
  public static final int DEFAULT_DEFAULT_ARTIFACT_PAGESIZE = 1000;

  /**
   * Max number of Artifacts that will be returned in a single (paged)
   * response
   */
  public static final String PARAM_MAX_ARTIFACT_PAGESIZE =
      PREFIX + "artifact.pagesize.max";
  public static final int DEFAULT_MAX_ARTIFACT_PAGESIZE = 2000;

  /**
   * Default number of AUIDs that will be returned in a single (paged)
   * response
   */
  public static final String PARAM_DEFAULT_AUID_PAGESIZE =
      PREFIX + "auid.pagesize.default";
  public static final int DEFAULT_DEFAULT_AUID_PAGESIZE = 1000;

  /**
   * Max number of AUIDs that will be returned in a single (paged)
   * response
   */
  public static final String PARAM_MAX_AUID_PAGESIZE =
      PREFIX + "auid.pagesize.max";
  public static final int DEFAULT_MAX_AUID_PAGESIZE = 2000;

  /**
   * Largest Artifact content that will be included in a response to a
   * getArtifactData call with includeContent == IF_SMALL
   */
  public static final String PARAM_SMALL_CONTENT_THRESHOLD =
      PREFIX + "smallContentThreshold";
  public static final long DEFAULT_SMALL_CONTENT_THRESHOLD = 4096;


  @Autowired
  LockssRepository repo;

  private final ObjectMapper objectMapper;

  private final HttpServletRequest request;

  // The artifact iterators used in pagination.
  private Map<Integer, Iterator<Artifact>> artifactIterators =
      new ConcurrentHashMap<>();

  // Timer callback for an artifact iterator timeout.
  private TimerQueue.Callback artifactIteratorTimeoutCallback =
      new TimerQueue.Callback() {
        public void timerExpired(Object cookie) {
          artifactIterators.remove((Integer) cookie);
        }
      };

  // The archival unit identifier iterators used in pagination.
  private Map<Integer, Iterator<String>> auidIterators =
      new ConcurrentHashMap<>();

  // Timer callback for an auid iterator timeout.
  private TimerQueue.Callback auidIteratorTimeoutCallback =
      new TimerQueue.Callback() {
        public void timerExpired(Object cookie) {
          auidIterators.remove((Integer) cookie);
        }
      };

  private int maxArtifactPageSize = DEFAULT_MAX_ARTIFACT_PAGESIZE;
  private int defaultArtifactPageSize = DEFAULT_DEFAULT_ARTIFACT_PAGESIZE;
  private int maxAuidPageSize = DEFAULT_MAX_AUID_PAGESIZE;
  private int defaultAuidPageSize = DEFAULT_DEFAULT_AUID_PAGESIZE;
  private long smallContentThreshold = DEFAULT_SMALL_CONTENT_THRESHOLD;

  /**
   * Constructor for autowiring.
   *
   * @param objectMapper An ObjectMapper for JSON processing.
   * @param request      An HttpServletRequest with the HTTP request.
   */
  @org.springframework.beans.factory.annotation.Autowired
  public CollectionsApiServiceImpl(ObjectMapper objectMapper,
                                   HttpServletRequest request) {
    this.objectMapper = objectMapper;
    this.request = request;
  }

  @javax.annotation.PostConstruct
  private void init() {
    setUpJms(JMS_BOTH,
        RestLockssRepository.REST_ARTIFACT_CACHE_ID,
        RestLockssRepository.REST_ARTIFACT_CACHE_TOPIC,
        new CacheInvalidateListener());
  }

  @Override
  public void setConfig(Configuration newConfig,
                        Configuration prevConfig,
                        Configuration.Differences changedKeys) {
    if (changedKeys.contains(PREFIX)) {
      defaultArtifactPageSize =
          newConfig.getInt(PARAM_DEFAULT_ARTIFACT_PAGESIZE,
              DEFAULT_DEFAULT_ARTIFACT_PAGESIZE);
      maxArtifactPageSize = newConfig.getInt(PARAM_MAX_ARTIFACT_PAGESIZE,
          DEFAULT_MAX_ARTIFACT_PAGESIZE);
      defaultAuidPageSize = newConfig.getInt(PARAM_DEFAULT_AUID_PAGESIZE,
          DEFAULT_DEFAULT_AUID_PAGESIZE);
      maxAuidPageSize = newConfig.getInt(PARAM_MAX_AUID_PAGESIZE,
          DEFAULT_MAX_AUID_PAGESIZE);
      smallContentThreshold =
          newConfig.getLong(PARAM_SMALL_CONTENT_THRESHOLD,
              DEFAULT_SMALL_CONTENT_THRESHOLD);
    }
  }

  /**
   * When JMS connection is established, tell clients to flush their
   * artifact cache to ensure that no stale cached artifacts.  (Normally
   * shouldn't matter, as artifact IDs are stable, even after an index
   * rebuild, but it's conceivable that the repository service that's
   * starting isn't the same one that was previously running at the same
   * address.)
   */
  @Override
  protected void jmsSetUpDone() {
    sendCacheFlush();
  }

  /**
   * GET /collections:
   * Returns a list of collection names managed by this repository.
   *
   * @return a List<String> with the collection names.
   */
  @Override
  public ResponseEntity<List<String>> getCollections() {
    String parsedRequest = String.format("requestUrl: %s",
        ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      List<String> collectionIds = IterableUtils.toList(repo.getCollectionIds());
      log.debug2("collectionIds = {}", collectionIds);
      return new ResponseEntity<>(collectionIds, HttpStatus.OK);
    } catch (IOException e) {
      String errorMessage = "Could not enumerate collection IDs";

      log.warn(errorMessage, e);
      log.warn("Parsed request: {}", parsedRequest);

      throw new LockssRestServiceException(
          LockssRestHttpException.ServerErrorType.DATA_ERROR,
          HttpStatus.INTERNAL_SERVER_ERROR,
          errorMessage, e, parsedRequest);
    }
  }

  /**
   * DELETE /collections/{collectionid}/artifacts/{artifactid}:
   * Deletes an artifact from a collection managed by this repository.
   *
   * @param collectionid A String with the name of the collection containing the artifact.
   * @param artifactid   A String with the Identifier of the artifact.
   * @return a {@code ResponseEntity<Void>}.
   */
  @Override
  public ResponseEntity<Void> deleteArtifact(String collectionid,
                                             String artifactid) {
    String parsedRequest = String.format(
        "collectionid: %s, artifactid: %s, requestUrl: %s",
        collectionid, artifactid, ServiceImplUtil.getFullRequestUrl(request));
    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      // Remove the artifact from the artifact store and index
      String key = artifactKey(collectionid, artifactid);
      repo.deleteArtifact(collectionid, artifactid);
      sendCacheInvalidate(ArtifactCache.InvalidateOp.Delete, key);
      return new ResponseEntity<>(HttpStatus.OK);

    } catch (LockssNoSuchArtifactIdException e) {
      return new ResponseEntity<Void>(HttpStatus.NOT_FOUND);

    } catch (IOException e) {
      String errorMessage = String.format(
          "IOException occurred while attempting to delete artifact from repository (artifactId: %s)",
          artifactid);

      log.warn(errorMessage, e);
      log.warn("Parsed request: {}", parsedRequest);

      throw new LockssRestServiceException(
          LockssRestHttpException.ServerErrorType.APPLICATION_ERROR,
          HttpStatus.INTERNAL_SERVER_ERROR,
          errorMessage, e, parsedRequest);
    }
  }

  /**
   * GET /collections/{collectionid}/artifacts/{artifactid}:
   * Retrieves an artifact from the repository.
   *
   * @param collectionid   A String with the name of the collection containing the artifact.
   * @param artifactid     A String with the Identifier of the artifact.
   * @param includeContent A {@link Boolean} indicating whether the artifact content part should be included in the
   *                       multipart response.
   * @return a {@link ResponseEntity} containing a {@link org.lockss.util.rest.multipart.MultipartResponse}.
   */
  @Override
  public ResponseEntity getArtifact(String collectionid, String artifactid, String includeContent) {
    String parsedRequest = String.format(
        "collectionid: %s, artifactid: %s, includeContent: %s, requestUrl: %s",
        collectionid, artifactid, includeContent, ServiceImplUtil.getFullRequestUrl(request)
    );

    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      log.debug2("Retrieving artifact [artifactId: {}, collectionId: {}]", artifactid, collectionid);

      // Retrieve the ArtifactData from the artifact store
      ArtifactData artifactData = repo.getArtifactData(collectionid, artifactid);

      // Break ArtifactData into multiparts
      MultiValueMap<String, Object> parts = generateMultipartResponseFromArtifactData(
          artifactData,
          LockssRepository.IncludeContent.valueOf(includeContent),
          smallContentThreshold
      );

      //// Return multiparts response entity
      return new ResponseEntity<MultiValueMap<String, Object>>(parts, HttpStatus.OK);

    } catch (LockssNoSuchArtifactIdException e) {
      // Translate to LockssRestServiceException and throw
      throw new LockssRestServiceException("Artifact not found", e)
          .setUtcTimestamp(LocalDateTime.now(ZoneOffset.UTC))
          .setHttpStatus(HttpStatus.NOT_FOUND)
          .setServletPath(request.getServletPath())
          .setServerErrorType(LockssRestHttpException.ServerErrorType.DATA_ERROR)
          .setParsedRequest(parsedRequest);

    } catch (IOException e) {
      String errorMessage = String.format(
          "Caught IOException while attempting to retrieve artifact from repository [artifactId: %s]",
          artifactid
      );

      log.warn(errorMessage, e);
      log.warn("Parsed request: {}", parsedRequest);

      throw new LockssRestServiceException(
          LockssRestHttpException.ServerErrorType.DATA_ERROR,
          HttpStatus.INTERNAL_SERVER_ERROR,
          errorMessage, e, parsedRequest);
    }
  }

  public static MultiValueMap<String, Object> generateMultipartResponseFromArtifactData(
      ArtifactData artifactData, LockssRepository.IncludeContent includeContent, long smallContentThreshold
  ) throws IOException {

    // Get artifact ID
    String artifactid = artifactData.getIdentifier().getId();

    // Holds multipart response parts
    MultiValueMap<String, Object> parts = new LinkedMultiValueMap<>();

    //// Add artifact repository properties multipart
    {
      // Part's headers
      HttpHeaders partHeaders = new HttpHeaders();
      partHeaders.setContentType(MediaType.APPLICATION_JSON);

      // Add repository properties multipart to multiparts list
      parts.add(
          RestLockssRepository.MULTIPART_ARTIFACT_REPO_PROPS,
          // FIXME: This artifact's repository properties basically describes an Artifact - use that instead?
          new HttpEntity<>(getArtifactRepositoryProperties(artifactData), partHeaders)
      );
    }

    //// Add artifact headers multipart
    {
      // Part's headers
      HttpHeaders partHeaders = new HttpHeaders();
      partHeaders.setContentType(MediaType.APPLICATION_JSON);

      // Add artifact headers multipart
      parts.add(
          RestLockssRepository.MULTIPART_ARTIFACT_HEADER,
          new HttpEntity<>(artifactData.getMetadata(), partHeaders)
      );
    }

    //// Add artifact HTTP status multipart if present
    if (artifactData.getHttpStatus() != null) {
      // Part's headers
      HttpHeaders partHeaders = new HttpHeaders();
      partHeaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);

      // Create resource containing HTTP status byte array
      Resource resource = new NamedByteArrayResource(
          artifactid,
          ArtifactDataUtil.getHttpStatusByteArray(artifactData.getHttpStatus())
      );

      // Add artifact headers multipart
      parts.add(
          RestLockssRepository.MULTIPART_ARTIFACT_HTTP_STATUS,
          new HttpEntity<>(resource, partHeaders)
      );
    }

    //// Add artifact content part if requested or if small enough
    if ((includeContent == LockssRepository.IncludeContent.ALWAYS) ||
        (includeContent == LockssRepository.IncludeContent.IF_SMALL
            && artifactData.getContentLength() <= smallContentThreshold)) {

      // Create content part headers
      HttpHeaders partHeaders = new HttpHeaders();
      partHeaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);
      partHeaders.setContentLength(artifactData.getContentLength());

      // Artifact content
      Resource resource = new NamedInputStreamResource(artifactid, artifactData.getInputStream());

      // Assemble content part and add to multiparts map
      parts.add(
          RestLockssRepository.MULTIPART_ARTIFACT_CONTENT,
          new HttpEntity<>(resource, partHeaders)
      );
    }

    return parts;
  }

  // FIXME: This is basically an Artifact - maybe use that instead?
  private static HttpHeaders getArtifactRepositoryProperties(ArtifactData ad) {
    HttpHeaders headers = new HttpHeaders();

    //// Artifact repository ID information headers
    ArtifactIdentifier id = ad.getIdentifier();
    headers.set(ArtifactConstants.ARTIFACT_ID_KEY, id.getId());
    headers.set(ArtifactConstants.ARTIFACT_COLLECTION_KEY, id.getCollection());
    headers.set(ArtifactConstants.ARTIFACT_AUID_KEY, id.getAuid());
    headers.set(ArtifactConstants.ARTIFACT_URI_KEY, id.getUri());
    headers.set(ArtifactConstants.ARTIFACT_VERSION_KEY, String.valueOf(id.getVersion()));

    //// Artifact repository state information headers if present
    if (ad.getArtifactRepositoryState() != null) {
      headers.set(
          ArtifactConstants.ARTIFACT_STATE_COMMITTED,
          String.valueOf(ad.getArtifactRepositoryState().getCommitted())
      );

      headers.set(
          ArtifactConstants.ARTIFACT_STATE_DELETED,
          String.valueOf(ad.getArtifactRepositoryState().getDeleted())
      );
    }

    //// Unclassified artifact repository headers
    headers.set(ArtifactConstants.ARTIFACT_LENGTH_KEY, String.valueOf(ad.getContentLength()));
    headers.set(ArtifactConstants.ARTIFACT_DIGEST_KEY, ad.getContentDigest());

//    headers.set(ArtifactConstants.ARTIFACT_ORIGIN_KEY, ???);
//    headers.set(ArtifactConstants.ARTIFACT_COLLECTION_DATE, ???);

    return headers;
  }

  /**
   * PUT /collections/{collectionid}/artifacts/{artifactid}:
   * Updates an artifact's properties
   * <p>
   * Currently limited to updating an artifact's committed status.
   *
   * @param collectionid A String with the name of the collection containing the artifact.
   * @param artifactid   A String with the Identifier of the artifact.
   * @param committed    A Boolean with the artifact committed status.
   * @return a {@code ResponseEntity<Artifact>}.
   */
  @Override
  public ResponseEntity updateArtifact(String collectionid,
                                       String artifactid, Boolean committed) {
    String parsedRequest = String.format(
        "collectionid: %s, artifactid: %s, committed: %s, requestUrl: %s",
        collectionid, artifactid, committed,
        ServiceImplUtil.getFullRequestUrl(request));
    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      if (committed == false) {
        // Not possible to uncommit an artifact
        throw new LockssRestServiceException("Cannot uncommit")
            .setServerErrorType(LockssRestHttpException.ServerErrorType.NONE)
            .setHttpStatus(HttpStatus.BAD_REQUEST)
            .setServletPath(request.getServletPath())
            .setParsedRequest(parsedRequest);
      }

      log.debug2(
          "Updating commit status for {} ({} -> {})",
          artifactid, repo.isArtifactCommitted(collectionid, artifactid), committed
      );

      // Commit the artifact
      Artifact updatedArtifact = repo.commitArtifact(collectionid, artifactid);

      // Broadcast an cache invalidate signal for this artifact
      sendCacheInvalidate(ArtifactCache.InvalidateOp.Commit, artifactKey(collectionid, artifactid));

      // Return the updated Artifact
      return new ResponseEntity<>(updatedArtifact, HttpStatus.OK);

    } catch (LockssNoSuchArtifactIdException e) {
      return new ResponseEntity<String>("Artifact not found", HttpStatus.NOT_FOUND);

    } catch (IOException e) {
      String errorMessage = String.format(
          "IOException occurred while attempting to update artifact metadata (artifactId: %s)",
          artifactid);

      log.warn(errorMessage, e);
      log.warn("Parsed request: {}", parsedRequest);

      throw new LockssRestServiceException(
          LockssRestHttpException.ServerErrorType.APPLICATION_ERROR,
          HttpStatus.INTERNAL_SERVER_ERROR,
          errorMessage, e, parsedRequest);
    }
  }

  /**
   * POST /collections/{collectionid}/artifacts:
   * Adds artifacts to the repository
   *
   * @param collectionid A String with the name of the collection containing the artifact.
   * @param auid         A String with the Archival Unit ID (AUID) of new artifact.
   * @param uri          A String with the URI represented by this artifact.
   * @param content      A MultipartFile with the artifact content.
   * @return a {@code ResponseEntity<Artifact>}.
   */
  @Override
  public ResponseEntity<Artifact> createArtifact(String collectionid,
                                                 String auid,
                                                 String uri,
                                                 MultipartFile content,
                                                 Long collectionDate) {

    String parsedRequest = String.format(
        "collectionid: %s, auid: %s, uri: %s, collectionDate: %s, requestUrl: %s",
        collectionid, auid, uri, collectionDate, ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    log.debug(String.format("Adding artifact %s, %s, %s",
        collectionid, auid, uri));

    log.trace(String.format("MultipartFile: Type: ArtifactData, Content-type: %s",
        content.getContentType()));

    // Check URI.
    validateUri(uri, parsedRequest);

    // Content-Type of the content part
    MediaType contentType = MediaType.parseMediaType(content.getContentType());

    // Only accept artifact encoded within an HTTP response. This is enforced by checking that
    // the artifact content part is of type "application/http;msgtype=response".
    if (!isHttpResponseType(contentType)) {
      String errorMessage = String.format(
          "Failed to add artifact; expected %s but got %s",
          APPLICATION_HTTP_RESPONSE,
          contentType);

      log.warn(errorMessage);
      log.warn("Parsed request: {}", parsedRequest);

      throw new LockssRestServiceException(errorMessage)
          .setServerErrorType(LockssRestHttpException.ServerErrorType.NONE)
          .setHttpStatus(HttpStatus.BAD_REQUEST)
          .setParsedRequest(parsedRequest);
    }

    try {
      //// Convert multipart stream to ArtifactData
      ArtifactData artifactData =
          ArtifactDataFactory.fromHttpResponseStream(content.getInputStream());

      //// Set ArtifactData properties from the POST request

      //TODO: FIX THIS CALL
      ArtifactIdentifier id =
          new ArtifactIdentifier(collectionid, auid, uri, 0);
      artifactData.setIdentifier(id);

      artifactData.setContentLength(content.getSize());

      // Set artifact collection date if provided
      if (collectionDate != null) {
        artifactData.setCollectionDate(collectionDate);
      }

      //// Add artifact to internal repository
      try {
        Artifact artifact = repo.addArtifact(artifactData);
        log.debug("Wrote artifact to {}", artifact.getStorageUrl());
        return new ResponseEntity<>(artifact, HttpStatus.OK);

      } catch (IOException e) {
        String errorMessage =
            "Caught IOException while attempting to add an artifact to the repository";

        log.warn(errorMessage, e);
        log.warn("Parsed request: {}", parsedRequest);

        throw new LockssRestServiceException(
            LockssRestHttpException.ServerErrorType.DATA_ERROR,
            HttpStatus.INTERNAL_SERVER_ERROR,
            errorMessage, e, parsedRequest);
      }

    } catch (IOException e) {
      // Thrown by ArtifactDataFactory.fromHttpResponseStream(InputStream)
      throw new HttpMessageNotReadableException("Could not read artifact data from content part", e);
    }
  }

  /**
   * GET /collections/{collectionid}/aus/{auid}/artifacts: Get a list with all
   * the artifacts in a collection and Archival Unit or a pageful of the list
   * defined by the continuation token and size.
   *
   * @param collectionid       A String with the name of the collection
   *                           containing the artifact.
   * @param auid               A String with the Archival Unit ID (AUID) of
   *                           artifact.
   * @param url                A String with the URL contained by the artifacts.
   * @param urlPrefix          A String with the prefix to be matched by the
   *                           artifact URLs.
   * @param version            An Integer with the version of the URL contained
   *                           by the artifacts.
   * @param includeUncommitted A boolean with the indication of whether
   *                           uncommitted artifacts should be returned.
   * @param limit              An Integer with the maximum number of artifacts
   *                           to be returned.
   * @param continuationToken  A String with the continuation token of the next
   *                           page of artifacts to be returned.
   * @return a {@code ResponseEntity<ArtifactPageInfo>} with the requested
   * artifacts.
   */
  @Override
  public ResponseEntity<ArtifactPageInfo> getArtifacts(String collectionid,
                                                       String auid, String url, String urlPrefix, String version,
                                                       Boolean includeUncommitted, Integer limit, String continuationToken) {

    String parsedRequest = String.format("collectionid: %s, auid: %s, url: %s, "
            + "urlPrefix: %s, version: %s, includeUncommitted: %s, limit: %s, "
            + "continuationToken: %s, requestUrl: %s",
        collectionid, auid, url, urlPrefix, version, includeUncommitted, limit,
        continuationToken, ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    Integer requestLimit = limit;
    limit = validateLimit(requestLimit, defaultArtifactPageSize,
        maxArtifactPageSize, parsedRequest);

    // Parse the request continuation token.
    ArtifactContinuationToken requestAct = null;

    try {
      requestAct = new ArtifactContinuationToken(continuationToken);
      log.trace("requestAct = {}", requestAct);
    } catch (IllegalArgumentException iae) {
      String message = "Invalid continuation token '" + continuationToken + "'";
      log.warn(message);

      throw new LockssRestServiceException(
          LockssRestHttpException.ServerErrorType.NONE,
          HttpStatus.BAD_REQUEST,
          message, parsedRequest);
    }

    try {
      boolean isLatestVersion =
          version == null || version.toLowerCase().equals("latest");
      log.trace("isLatestVersion = {}", isLatestVersion);

      boolean isAllVersions =
          version != null && version.toLowerCase().equals("all");
      log.trace("isAllVersions = {}", isAllVersions);

      if (urlPrefix != null && url != null) {
        String errorMessage =
            "The 'urlPrefix' and 'url' arguments are mutually exclusive";

        log.warn(errorMessage);
        log.warn("Parsed request: {}", parsedRequest);

        throw new LockssRestServiceException(
            LockssRestHttpException.ServerErrorType.NONE, HttpStatus.BAD_REQUEST,
            errorMessage, parsedRequest);
      }

      boolean isSpecificVersion = !isAllVersions && !isLatestVersion;
      log.trace("isSpecificVersion = {}", isSpecificVersion);
      boolean isAllUrls = url == null && urlPrefix == null;
      log.trace("isAllUrls = {}", isAllUrls);

      if (isSpecificVersion && (isAllUrls || urlPrefix != null)) {
        String errorMessage =
            "A specific 'version' argument requires a 'url' argument";

        log.warn(errorMessage);
        log.warn("Parsed request: {}", parsedRequest);

        throw new LockssRestServiceException(
            LockssRestHttpException.ServerErrorType.NONE, HttpStatus.BAD_REQUEST,
            errorMessage, parsedRequest);
      }

      boolean includeUncommittedValue = includeUncommitted != null
          && includeUncommitted.booleanValue();
      log.trace("includeUncommittedValue = {}", includeUncommittedValue);

      if (!isSpecificVersion && includeUncommittedValue) {
        String errorMessage =
            "Including an uncommitted artifact requires a specific 'version' argument";

        log.warn(errorMessage);
        log.warn("Parsed request: {}", parsedRequest);

        throw new LockssRestServiceException(
            LockssRestHttpException.ServerErrorType.NONE, HttpStatus.BAD_REQUEST,
            errorMessage, parsedRequest);
      }

      int numericVersion = 0;

      if (isSpecificVersion) {
        try {
          numericVersion = Integer.parseInt(version);
          log.trace("numericVersion = {}", numericVersion);

          if (numericVersion <= 0) {
            String errorMessage =
                "The 'version' argument is not a positive integer";

            log.warn(errorMessage);
            log.warn("Parsed request: {}", parsedRequest);

            throw new LockssRestServiceException(
                LockssRestHttpException.ServerErrorType.NONE, HttpStatus.BAD_REQUEST,
                errorMessage, parsedRequest);
          }
        } catch (NumberFormatException nfe) {
          String errorMessage =
              "The 'version' argument is not a positive integer";

          log.warn(errorMessage);
          log.warn("Parsed request: {}", parsedRequest);

          throw new LockssRestServiceException(
              LockssRestHttpException.ServerErrorType.NONE, HttpStatus.BAD_REQUEST,
              errorMessage, parsedRequest);
        }
      }

      // Check that the collection exists.
      ServiceImplUtil.validateCollectionId(repo, collectionid, parsedRequest);

      // Check that the Archival Unit exists.
      validateAuId(collectionid, auid, parsedRequest);

      Iterable<Artifact> artifactIterable = null;
      List<Artifact> artifacts = new ArrayList<>();
      Iterator<Artifact> iterator = null;
      boolean missingIterator = false;

      // Get the iterator hash code (if any) used to provide a previous page
      // of results.
      Integer iteratorHashCode = requestAct.getIteratorHashCode();

      // Check whether this request is for a previous page of results.
      if (iteratorHashCode != null) {
        // Yes: Get the iterator (if any) used to provide a previous page of
        // results.
        iterator = artifactIterators.remove(iteratorHashCode);
        missingIterator = iterator == null;
      }

      if (isAllUrls && isAllVersions) {
        log.trace("All versions of all URLs");
        if (iterator == null) {
          artifactIterable = repo.getArtifactsAllVersions(collectionid, auid);
        }
      } else if (urlPrefix != null && isAllVersions) {
        log.trace("All versions of all URLs matching a prefix");
        if (iterator == null) {
          artifactIterable = repo.getArtifactsWithPrefixAllVersions(
              collectionid, auid, urlPrefix);
        }
      } else if (url != null && isAllVersions) {
        log.trace("All versions of a URL");
        if (iterator == null) {
          artifactIterable =
              repo.getArtifactsAllVersions(collectionid, auid, url);
        }
      } else if (isAllUrls && isLatestVersion) {
        log.trace("Latest versions of all URLs");
        if (iterator == null) {
          artifactIterable = repo.getArtifacts(collectionid, auid);
        }
      } else if (urlPrefix != null && isLatestVersion) {
        log.trace("Latest versions of all URLs matching a prefix");
        if (iterator == null) {
          artifactIterable =
              repo.getArtifactsWithPrefix(collectionid, auid, urlPrefix);
        }
      } else if (url != null && isLatestVersion) {
        log.trace("Latest version of a URL");
        Artifact artifact = repo.getArtifact(collectionid, auid, url);
        log.trace("artifact = {}", artifact);

        if (artifact != null) {
          artifacts.add(artifact);
        }
      } else if (url != null && numericVersion > 0) {
        log.trace("Given version of a URL");
        log.trace("collectionid = {}", collectionid);
        log.trace("auid = {}", auid);
        log.trace("url = {}", url);
        log.trace("numericVersion = {}", numericVersion);
        log.trace("includeUncommittedValue = {}", includeUncommittedValue);
        Artifact artifact = repo.getArtifactVersion(collectionid, auid, url,
            numericVersion, includeUncommittedValue);
        log.trace("artifact = {}", artifact);

        if (artifact != null) {
          artifacts.add(artifact);
        }
      } else {
        String errorMessage = "The request could not be understood";

        log.warn(errorMessage);
        log.warn("Parsed request: {}", parsedRequest);

        throw new LockssRestServiceException(
            LockssRestHttpException.ServerErrorType.NONE, HttpStatus.BAD_REQUEST,
            errorMessage, parsedRequest);
      }

      ArtifactContinuationToken responseAct = null;

      // Check whether an iterator is involved in obtaining the response.
      if (iterator != null || artifactIterable != null) {
        // Yes: Check whether a new iterator is needed.
        if (iterator == null) {
          // Yes: Get the iterator pointing to the first page of results.
          iterator = artifactIterable.iterator();

          // Set up the iterator timeout.
          TimerQueue.schedule(Deadline.in(48 * TimeUtil.HOUR),
              artifactIteratorTimeoutCallback, iterator.hashCode());

          // Check whether the artifacts provided in a previous response need to
          // be skipped.
          if (missingIterator) {
            // Yes: Initialize an artifact with properties from the last one
            // already returned in the previous page of results.
            Artifact lastArtifact = new Artifact();
            lastArtifact.setCollection(requestAct.getCollectionId());
            lastArtifact.setAuid(requestAct.getAuid());
            lastArtifact.setUri(requestAct.getUri());
            lastArtifact.setVersion(requestAct.getVersion());

            // Loop through the artifacts skipping those already returned
            // through a previous response.
            while (iterator.hasNext()) {
              Artifact artifact = iterator.next();

              // Check whether this artifact comes after the last one returned
              // on the previous response for this operation.
              if (ArtifactComparators.BY_URI_BY_DECREASING_VERSION
                  .compare(artifact, lastArtifact) > 0) {
                // Yes: Add this artifact to the results.
                artifacts.add(artifact);

                // Add the rest of the artifacts to the results for this
                // response separately.
                break;
              }
            }
          }
        }

        // Populate the the rest of the results for this response.
        populateArtifacts(iterator, limit, artifacts);

        // Check whether the iterator may be used in the future to provide more
        // results.
        if (iterator.hasNext()) {
          // Yes: Store it locally.
          iteratorHashCode = iterator.hashCode();
          artifactIterators.put(iteratorHashCode, iterator);

          // Create the response continuation token.
          Artifact lastArtifact = artifacts.get(artifacts.size() - 1);
          responseAct = new ArtifactContinuationToken(
              lastArtifact.getCollection(), lastArtifact.getAuid(),
              lastArtifact.getUri(), lastArtifact.getVersion(),
              iteratorHashCode);
          log.trace("responseAct = {}", responseAct);
        }
      }

      log.trace("artifacts.size() = {}", artifacts.size());

      PageInfo pageInfo = new PageInfo();
      pageInfo.setResultsPerPage(artifacts.size());

      // Get the current link.
      StringBuffer curLinkBuffer = request.getRequestURL();

      if (request.getQueryString() != null
          && !request.getQueryString().trim().isEmpty()) {
        curLinkBuffer.append("?").append(request.getQueryString());
      }

      String curLink = curLinkBuffer.toString();
      log.trace("curLink = {}", curLink);

      pageInfo.setCurLink(curLink);

      // Check whether there is a response continuation token.
      if (responseAct != null) {
        // Yes.
        continuationToken = responseAct.toWebResponseContinuationToken();
        pageInfo.setContinuationToken(continuationToken);

        // Start building the next link.
        StringBuffer nextLinkBuffer = request.getRequestURL();
        boolean hasQueryParameters = false;

        if (curLink.indexOf("limit=") > 0) {
          nextLinkBuffer.append("?limit=").append(requestLimit);
          hasQueryParameters = true;
        }

        if (url != null) {
          if (!hasQueryParameters) {
            nextLinkBuffer.append("?");
            hasQueryParameters = true;
          } else {
            nextLinkBuffer.append("&");
          }

          nextLinkBuffer.append("url=").append(UrlUtil.encodeUrl(url));
        }

        if (urlPrefix != null) {
          if (!hasQueryParameters) {
            nextLinkBuffer.append("?");
            hasQueryParameters = true;
          } else {
            nextLinkBuffer.append("&");
          }

          nextLinkBuffer.append("urlPrefix=")
              .append(UrlUtil.encodeUrl(urlPrefix));
        }

        if (version != null) {
          if (!hasQueryParameters) {
            nextLinkBuffer.append("?");
            hasQueryParameters = true;
          } else {
            nextLinkBuffer.append("&");
          }

          nextLinkBuffer.append("version=").append(version);
        }

        if (includeUncommitted != null) {
          if (!hasQueryParameters) {
            nextLinkBuffer.append("?");
            hasQueryParameters = true;
          } else {
            nextLinkBuffer.append("&");
          }

          nextLinkBuffer.append("includeUncommitted=")
              .append(includeUncommitted);
        }

        continuationToken = pageInfo.getContinuationToken();

        if (continuationToken != null) {
          if (!hasQueryParameters) {
            nextLinkBuffer.append("?");
            hasQueryParameters = true;
          } else {
            nextLinkBuffer.append("&");
          }

          nextLinkBuffer.append("continuationToken=")
              .append(UrlUtil.encodeUrl(continuationToken));
        }

        String nextLink = nextLinkBuffer.toString();
        log.trace("nextLink = {}", nextLink);

        pageInfo.setNextLink(nextLink);
      }

      ArtifactPageInfo artifactPageInfo = new ArtifactPageInfo();
      artifactPageInfo.setArtifacts(artifacts);
      artifactPageInfo.setPageInfo(pageInfo);
      log.trace("artifactPageInfo = {}", artifactPageInfo);

      log.debug2("Returning OK.");
      return new ResponseEntity<>(artifactPageInfo, HttpStatus.OK);

    } catch (IOException e) {
      throw new LockssRestServiceException(LockssRestHttpException.ServerErrorType.DATA_ERROR,
          HttpStatus.INTERNAL_SERVER_ERROR,
          "IOException", e, parsedRequest);

//    } catch (Exception e) {
//      String errorMessage =
//          "Unexpected exception caught while attempting to retrieve artifacts";
//
//      log.warn(errorMessage, e);
//      log.warn("Parsed request: {}", parsedRequest);
//
//      throw new LockssRestServiceException(HttpStatus.INTERNAL_SERVER_ERROR,
//          errorMessage, e, parsedRequest);
    }
  }

  /**
   * GET /collections/{collectionid}/aus/{auid}/size:
   * Get the size of Archival Unit artifacts in a collection.
   *
   * @param collectionid A String with the name of the collection containing the Archival
   *                     Unit.
   * @param auid         A String with the Archival Unit ID (AUID).
   * @param url          A String with the URL contained by the artifacts.
   * @param urlPrefix    A String with the prefix to be matched by the artifact URLs.
   * @param version      An Integer with the version of the URL contained by the artifacts.
   * @return a Long{@code ResponseEntity<Long>}.
   */
  @Override
  public ResponseEntity<Long> getArtifactsSize(String collectionid, String auid,
                                               String url, String urlPrefix, String version) {
    String parsedRequest = String.format(
        "collectionid: %s, auid: %s, url: %s, urlPrefix: %s, version: %s, requestUrl: %s",
        collectionid, auid, url, urlPrefix, version,
        ServiceImplUtil.getFullRequestUrl(request));
    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      boolean isLatestVersion =
          version == null || version.toLowerCase().equals("latest");

      boolean isAllVersions =
          version != null && version.toLowerCase().equals("all");

      if (urlPrefix != null && url != null) {
        String errorMessage =
            "The 'urlPrefix' and 'url' arguments are mutually exclusive";

        log.warn(errorMessage);
        log.warn("Parsed request: {}", parsedRequest);

        throw new LockssRestServiceException(
            LockssRestHttpException.ServerErrorType.NONE,
            HttpStatus.BAD_REQUEST,
            errorMessage, parsedRequest);
      }

      boolean isSpecificVersion = !isAllVersions && !isLatestVersion;
      boolean isAllUrls = url == null && urlPrefix == null;

      if (isSpecificVersion && (isAllUrls || urlPrefix != null)) {
        String errorMessage =
            "A specific 'version' argument requires a 'url' argument";

        log.warn(errorMessage);
        log.warn("Parsed request: {}", parsedRequest);

        throw new LockssRestServiceException(
            LockssRestHttpException.ServerErrorType.NONE,
            HttpStatus.BAD_REQUEST,
            errorMessage, parsedRequest);
      }

      int numericVersion = 0;

      if (isSpecificVersion) {
        try {
          numericVersion = Integer.parseInt(version);

          if (numericVersion <= 0) {
            String errorMessage =
                "The 'version' argument is not a positive integer";

            log.warn(errorMessage);
            log.warn("Parsed request: {}", parsedRequest);

            throw new LockssRestServiceException(
                LockssRestHttpException.ServerErrorType.NONE,
                HttpStatus.BAD_REQUEST,
                errorMessage, parsedRequest);
          }
        } catch (NumberFormatException nfe) {
          String errorMessage =
              "The 'version' argument is not a positive integer";

          log.warn(errorMessage);
          log.warn("Parsed request: {}", parsedRequest);

          throw new LockssRestServiceException(
              LockssRestHttpException.ServerErrorType.NONE,
              HttpStatus.BAD_REQUEST,
              errorMessage, parsedRequest);
        }
      }

      // Check that the collection exists.
      ServiceImplUtil.validateCollectionId(repo, collectionid, parsedRequest);

      // Check that the Archival Unit exists.
      validateAuId(collectionid, auid, parsedRequest);

      Long result = repo.auSize(collectionid, auid);
      log.debug2("result = {}", result);
      return new ResponseEntity<>(result, HttpStatus.OK);
    } catch (LockssRestServiceException lre) {
      // Let it cascade to the controller advice exception handler.
      throw lre;
    } catch (Exception e) {
      String errorMessage =
          "Unexpected exception caught while attempting to get artifacts size";

      log.warn(errorMessage, e);
      log.warn("Parsed request: {}", parsedRequest);

      throw new LockssRestServiceException(
          LockssRestHttpException.ServerErrorType.APPLICATION_ERROR,
          HttpStatus.INTERNAL_SERVER_ERROR,
          errorMessage, e, parsedRequest);
    }
  }

  /**
   * GET /collections/{collectionid}/aus: Get all Archival Unit IDs (AUIDs) in a
   * collection or a pageful of the list defined by the continuation token and
   * size.
   *
   * @param collectionid      A String with the name of the collection
   *                          containing the archival units.
   * @param limit             An Integer with the maximum number of archival
   *                          unit identifiers to be returned.
   * @param continuationToken A String with the continuation token of the next
   *                          page of archival unit identifiers to be returned.
   * @return a {@code ResponseEntity<AuidPageInfo>}.
   */
  @Override
  public ResponseEntity<AuidPageInfo> getAus(String collectionid, Integer limit,
                                             String continuationToken) {
    String parsedRequest = String.format("collectionid: %s, requestUrl: %s",
        collectionid, ServiceImplUtil.getFullRequestUrl(request));
    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    Integer requestLimit = limit;
    limit = validateLimit(requestLimit, defaultAuidPageSize, maxAuidPageSize,
        parsedRequest);

    // Parse the request continuation token.
    AuidContinuationToken requestAct = null;

    try {
      requestAct = new AuidContinuationToken(continuationToken);
      log.trace("requestAct = {}", requestAct);
    } catch (IllegalArgumentException iae) {
      String message = "Invalid continuation token '" + continuationToken + "'";
      log.warn(message);
      throw new LockssRestServiceException(
          LockssRestHttpException.ServerErrorType.NONE,
          HttpStatus.BAD_REQUEST,
          message,
          parsedRequest);
    }

    try {
      // Check that the collection exists.
      ServiceImplUtil.validateCollectionId(repo, collectionid, parsedRequest);

      List<String> auids = new ArrayList<>();
      AuidContinuationToken responseAct = null;
      Iterator<String> iterator = null;

      // Get the iterator hash code (if any) used to provide a previous page
      // of results.
      Integer iteratorHashCode = requestAct.getIteratorHashCode();

      // Check whether this request is for the first page.
      if (iteratorHashCode == null) {
        // Yes: Get the iterator pointing to first page of results.
        iterator = repo.getAuIds(collectionid).iterator();

        // Set up the iterator timeout.
        TimerQueue.schedule(Deadline.in(48 * TimeUtil.HOUR),
            auidIteratorTimeoutCallback, iterator.hashCode());
      } else {
        // No: Get the iterator (if any) used to provide a previous page of
        // results.
        iterator = auidIterators.remove(iteratorHashCode);

        // Check whether the iterator was not found.
        if (iterator == null) {
          // Yes: This request is not for the first page of results, but the
          // iterator has been lost.
          String lastAuid = requestAct.getAuid();

          // Get the iterator pointing to first page of results.
          iterator = repo.getAuIds(collectionid).iterator();

          // Set up the iterator timeout.
          TimerQueue.schedule(Deadline.in(48 * TimeUtil.HOUR),
              auidIteratorTimeoutCallback, iterator.hashCode());

          // Loop through the auids skipping those already returned through a
          // previous response.
          while (iterator.hasNext()) {
            String auid = iterator.next();

            // Check whether this auid comes after the last one returned on the
            // previous response for this operation.
            if (auid.compareTo(lastAuid) > 0) {
              // Yes: Add this auid to the results.
              auids.add(auid);

              // Add the rest of the artifacts to the results for this response
              // separately.
              break;
            }
          }
        }
      }

      // Populate the results for this response.
      populateAus(iterator, limit, auids);

      // Check whether the iterator may be used in the future to provide more
      // results.
      if (iterator.hasNext()) {
        // Yes: Store it locally.
        iteratorHashCode = iterator.hashCode();
        auidIterators.put(iteratorHashCode, iterator);

        // Create the response continuation token.
        responseAct = new AuidContinuationToken(auids.get(auids.size() - 1),
            iteratorHashCode);
        log.trace("responseAct = {}", responseAct);
      }

      log.trace("auids.size() = {}", auids.size());

      PageInfo pageInfo = new PageInfo();
      pageInfo.setResultsPerPage(auids.size());

      // Get the current link.
      StringBuffer curLinkBuffer = request.getRequestURL();

      if (request.getQueryString() != null
          && !request.getQueryString().trim().isEmpty()) {
        curLinkBuffer.append("?").append(request.getQueryString());
      }

      String curLink = curLinkBuffer.toString();
      log.trace("curLink = {}", curLink);

      pageInfo.setCurLink(curLink);

      // Check whether there is a response continuation token.
      if (responseAct != null) {
        // Yes.
        continuationToken = responseAct.toWebResponseContinuationToken();
        pageInfo.setContinuationToken(continuationToken);

        // Start building the next link.
        StringBuffer nextLinkBuffer = request.getRequestURL();
        boolean hasQueryParameters = false;

        if (curLink.indexOf("limit=") > 0) {
          nextLinkBuffer.append("?limit=").append(requestLimit);
          hasQueryParameters = true;
        }

        if (continuationToken != null) {
          if (!hasQueryParameters) {
            nextLinkBuffer.append("?");
            hasQueryParameters = true;
          } else {
            nextLinkBuffer.append("&");
          }

          nextLinkBuffer.append("continuationToken=")
              .append(UrlUtil.encodeUrl(continuationToken));
        }

        String nextLink = nextLinkBuffer.toString();
        log.trace("nextLink = {}", nextLink);

        pageInfo.setNextLink(nextLink);
      }

      AuidPageInfo auidPageInfo = new AuidPageInfo();
      auidPageInfo.setAuids(auids);
      auidPageInfo.setPageInfo(pageInfo);
      log.trace("auidPageInfo = {}", auidPageInfo);

      log.debug2("Returning OK.");
      return new ResponseEntity<>(auidPageInfo, HttpStatus.OK);
//    } catch (LockssRestServiceException lre) {
//      // Let it cascade to the controller advice exception handler.
//      throw lre;
    } catch (IOException e) {
      String errorMessage =
          "Unexpected exception caught while attempting to get AU ids";

      log.warn(errorMessage, e);
      log.warn("Parsed request: {}", parsedRequest);

      throw new LockssRestServiceException(HttpStatus.INTERNAL_SERVER_ERROR,
          errorMessage, e, parsedRequest);
    }
  }

  private static Boolean isHttpResponseType(MediaType type) {
    return (APPLICATION_HTTP_RESPONSE.isCompatibleWith(type) && (type
        .getParameters().equals(APPLICATION_HTTP_RESPONSE.getParameters())));
  }

  private void validateAuId(String collectionid, String auid,
                            String parsedRequest) throws IOException {

    // XXX Disabled.  It's normal for clients to ask about AUs that haven't
    // had any content stored in them.  Also, this is likely to trigger
    // CMEs in VolatileArtifactIndex's iterators in tests and dev env.
    if (true) return;

    log.debug2("collectionid = '{}'", collectionid);
    log.debug2("auid = '{}'", auid);
    log.debug2("parsedRequest = '{}'", parsedRequest);

    if (!StreamSupport.stream(repo.getAuIds(collectionid).spliterator(), false)
        .anyMatch(name -> auid.equals(name))) {
      String errorMessage = "The archival unit has no artifacts (possibly because it hasn't been collected yet)";
      log.warn(errorMessage);
      log.warn("Parsed request: {}", parsedRequest);

      throw new LockssRestServiceException(
          LockssRestHttpException.ServerErrorType.DATA_ERROR,
          HttpStatus.NOT_FOUND, errorMessage,
          parsedRequest);
    }

    log.debug2("auid '{}' is valid.", auid);
  }

  private void validateUri(String uri, String parsedRequest) {
    log.debug2("uri = '{}'", uri);
    log.debug2("parsedRequest = '{}'", parsedRequest);
    if (uri.isEmpty()) {
      String errorMessage = "The URI has not been provided";
      log.warn(errorMessage);
      log.warn("Parsed request: {}", parsedRequest);

      throw new LockssRestServiceException(
          LockssRestHttpException.ServerErrorType.NONE, HttpStatus.BAD_REQUEST,
          errorMessage, parsedRequest);
    }

    log.debug2("uri '{}' is valid.", uri);
  }

  @Override
  public Optional<ObjectMapper> getObjectMapper() {
    return Optional.ofNullable(objectMapper);
  }

  @Override
  public Optional<HttpServletRequest> getRequest() {
    return Optional.ofNullable(request);
  }

  String artifactKey(String collectionid, String artifactid)
      throws IOException {
    Artifact art = repo.getArtifactFromId(artifactid);
    if (art != null) {
      return art.makeKey();
    } else {
      log.error("Expected artifact not found, can't send invalidate: {}",
          artifactid);
      return null;
    }
  }

  protected void sendCacheInvalidate(ArtifactCache.InvalidateOp op,
                                     String key) {
    if (jmsProducer != null && key != null) {
      Map<String, Object> map = new HashMap<>();
      map.put(RestLockssRepository.REST_ARTIFACT_CACHE_MSG_ACTION,
          RestLockssRepository.REST_ARTIFACT_CACHE_MSG_ACTION_INVALIDATE);
      map.put(RestLockssRepository.REST_ARTIFACT_CACHE_MSG_OP, op.toString());
      map.put(RestLockssRepository.REST_ARTIFACT_CACHE_MSG_KEY, key);
      try {
        jmsProducer.sendMap(map);
      } catch (JMSException e) {
        log.error("Couldn't send cache invalidate notification", e);
      }
    }
  }

  protected void sendCacheFlush() {
    if (jmsProducer != null) {
      Map<String, Object> map = new HashMap<>();
      map.put(RestLockssRepository.REST_ARTIFACT_CACHE_MSG_ACTION,
          RestLockssRepository.REST_ARTIFACT_CACHE_MSG_ACTION_FLUSH);
      try {
        jmsProducer.sendMap(map);
      } catch (JMSException e) {
        log.error("Couldn't send cache flush notification", e);
      }
    }
  }

  /**
   * Populates the artifacts to be included in the response.
   *
   * @param iterator  An Iterator<Artifact> with the artifact source iterator.
   * @param limit     An Integer with the maximum number of artifacts to be
   *                  included in the response.
   * @param artifacts A List<Artifact> with the artifacts to be included in the
   *                  response.
   */
  private void populateArtifacts(Iterator<Artifact> iterator, Integer limit,
                                 List<Artifact> artifacts) {
    log.debug2("limit = {}, artifacts = {}", limit, artifacts);
    int artifactCount = artifacts.size();

    // Loop through as many artifacts that exist and are requested.
    while (artifactCount < limit && iterator.hasNext()) {
      // Add this artifact to the results.
      artifacts.add(iterator.next());
      artifactCount++;
    }
  }

  /**
   * Populates the auids to be included in the response.
   *
   * @param iterator An Iterator<String> with the auid source iterator.
   * @param limit    An Integer with the maximum number of auids to be included
   *                 in the response.
   * @param auids    A List<String> with the auids to be included in the
   *                 response.
   */
  private void populateAus(Iterator<String> iterator, Integer limit,
                           List<String> auids) {
    log.debug2("limit = {}, auids = {}", limit, auids);
    int auidCount = auids.size();

    // Loop through as many auids that exist and are requested.
    while (auidCount < limit && iterator.hasNext()) {
      // Add this auid to the results.
      auids.add(iterator.next());
      auidCount++;
    }
  }

  /**
   * Validates the page size specified in the request.
   *
   * @param requestLimit An Integer with the page size specified in the request.
   * @param defaultValue An int with the value to be used when no page size is
   *                     specified in the request.
   * @param maxValue     An int with the maximum allowed value for the page
   *                     size.
   * @return an int with the validated value for the page size.
   */
  static int validateLimit(Integer requestLimit, int defaultValue, int maxValue,
                           String parsedRequest) {
    log.debug2("requestLimit = {}, defaultValue = {}, maxValue = {}",
        requestLimit, defaultValue, maxValue);

    // Check whether it's not a positive integer.
    if (requestLimit != null && requestLimit.intValue() <= 0) {
      // Yes: Report the problem.
      String message =
          "Limit of requested items must be a positive integer; it was '"
              + requestLimit + "'";
      log.warn(message);

      throw new LockssRestServiceException(
          LockssRestHttpException.ServerErrorType.NONE, HttpStatus.BAD_REQUEST,
          message, parsedRequest);
    }

    // No: Get the result.
    int result = requestLimit == null ?
        Math.min(defaultValue, maxValue) : Math.min(requestLimit, maxValue);
    log.debug2("result = {}", result);
    return result;
  }

  // Respond to ECHO requests from client caches.  This verifies that the
  // service supports sending cache invalidate messages, so it is safe to
  // enable the Artifact cache

  private class CacheInvalidateListener implements MessageListener {

    @Override
    public void onMessage(Message message) {
      try {
        Map<String, String> msgMap =
            (Map<String, String>) JmsUtil.convertMessage(message);
        String action =
            msgMap.get(RestLockssRepository.REST_ARTIFACT_CACHE_MSG_ACTION);
        String key =
            msgMap.get(RestLockssRepository.REST_ARTIFACT_CACHE_MSG_KEY);
        log.debug2("Received Artifact cache message: {}: {}", action, key);
        if (action != null) {
          switch (action) {
            case RestLockssRepository.REST_ARTIFACT_CACHE_MSG_ACTION_ECHO:
              sendPingResponse(key);
              break;
            case RestLockssRepository.REST_ARTIFACT_CACHE_MSG_ACTION_INVALIDATE:
            case RestLockssRepository.REST_ARTIFACT_CACHE_MSG_ACTION_ECHO_RESP:
            case RestLockssRepository.REST_ARTIFACT_CACHE_MSG_ACTION_FLUSH:
              // expected, ignore
              break;
            default:
              log.warn("Unknown message action: " + action);
          }
        }
      } catch (JMSException | RuntimeException e) {
        log.error("Malformed Artifact cache message: " + message, e);
      }
    }
  }

  protected void sendPingResponse(String key) {
    if (jmsProducer != null) {
      Map<String, Object> map = new HashMap<>();
      map.put(RestLockssRepository.REST_ARTIFACT_CACHE_MSG_ACTION,
          RestLockssRepository.REST_ARTIFACT_CACHE_MSG_ACTION_ECHO_RESP);
      map.put(RestLockssRepository.REST_ARTIFACT_CACHE_MSG_KEY, key);
      try {
        jmsProducer.sendMap(map);
      } catch (JMSException e) {
        log.error("Couldn't send cache invalidate notification", e);
      }
    }
  }
}
