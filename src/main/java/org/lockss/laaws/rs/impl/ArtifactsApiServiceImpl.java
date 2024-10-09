package org.lockss.laaws.rs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.lockss.config.Configuration;
import org.lockss.laaws.rs.api.ArtifactsApiDelegate;
import org.lockss.laaws.rs.multipart.LockssMultipartHttpServletRequest;
import org.lockss.log.L4JLogger;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.storage.warc.WarcArtifactDataUtil;
import org.lockss.spring.base.BaseSpringApiServiceImpl;
import org.lockss.spring.base.LockssConfigurableService;
import org.lockss.spring.error.LockssRestServiceException;
import org.lockss.util.StringUtil;
import org.lockss.util.TimerQueue;
import org.lockss.util.UrlUtil;
import org.lockss.util.jms.JmsUtil;
import org.lockss.util.rest.exception.LockssRestHttpException;
import org.lockss.util.rest.multipart.MultipartResponse;
import org.lockss.util.rest.repo.LockssNoSuchArtifactIdException;
import org.lockss.util.rest.repo.LockssRepository;
import org.lockss.util.rest.repo.RestLockssRepository;
import org.lockss.util.rest.repo.model.*;
import org.lockss.util.rest.repo.util.ArtifactCache;
import org.lockss.util.rest.repo.util.ArtifactComparators;
import org.lockss.util.rest.repo.util.ArtifactConstants;
import org.lockss.util.rest.repo.util.ArtifactDataUtil;
import org.lockss.util.time.Deadline;
import org.lockss.util.time.TimeBase;
import org.lockss.util.time.TimeUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;
import org.springframework.web.multipart.MultipartFile;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

import static org.lockss.laaws.rs.impl.ServiceImplUtil.populateArtifacts;
import static org.lockss.laaws.rs.impl.ServiceImplUtil.validateLimit;

@Service
public class ArtifactsApiServiceImpl extends BaseSpringApiServiceImpl
    implements ArtifactsApiDelegate, LockssConfigurableService {
  private static L4JLogger log = L4JLogger.getLogger();

  public static final String APPLICATION_HTTP_RESPONSE_VALUE =
      "application/http;msgtype=response";

  public static final MediaType APPLICATION_HTTP_RESPONSE =
      MediaType.parseMediaType(APPLICATION_HTTP_RESPONSE_VALUE);

  @Autowired
  BaseLockssRepository repo;

  @Autowired
  ObjectMapper objMapper;

  private final HttpServletRequest request;

  private Set<String> bulkAuids = new CopyOnWriteArraySet<>();

  // These maps are initialized to normal maps just in case they're
  // accessed before setConfig() is called and creates the official
  // PassiveExpiringMaps.  I don't think the service methods here can
  // be called before the config is loaded, but this is easy insurance
  // that nothing seriously bad happen if they are.

  // The artifact iterators used in pagination.
  private Map<Integer, Iterator<Artifact>> artifactIterators = null;

  @Autowired
  public ArtifactsApiServiceImpl(HttpServletRequest request) {
    this.request = request;
  }

  ////////////////////////////////////////////////////////////////////////////////
  // PARAMS //////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////

  public static final String PREFIX = "org.lockss.repository.";

  /**
   * Largest Artifact content that will be included in a response to a
   * getArtifactData call with includeContent == IF_SMALL
   */
  public static final String PARAM_SMALL_CONTENT_THRESHOLD = PREFIX + "smallContentThreshold";
  public static final long DEFAULT_SMALL_CONTENT_THRESHOLD = 4096;
  private long smallContentThreshold = DEFAULT_SMALL_CONTENT_THRESHOLD;

  /**
   * Default number of Artifacts that will be returned in a single (paged)
   * response
   */
  public static final String PARAM_DEFAULT_ARTIFACT_PAGESIZE = PREFIX + "artifact.pagesize.default";
  public static final int DEFAULT_DEFAULT_ARTIFACT_PAGESIZE = 1000;
  private int defaultArtifactPageSize = DEFAULT_DEFAULT_ARTIFACT_PAGESIZE;

  /**
   * Max number of Artifacts that will be returned in a single (paged)
   * response
   */
  public static final String PARAM_MAX_ARTIFACT_PAGESIZE = PREFIX + "artifact.pagesize.max";
  public static final int DEFAULT_MAX_ARTIFACT_PAGESIZE = 2000;
  private int maxArtifactPageSize = DEFAULT_MAX_ARTIFACT_PAGESIZE;

  /**
   * Interval after which unused Artifact iterator continuations will
   * be discarded.  Change requires restart to take effect.
   */
  public static final String PARAM_ARTIFACT_ITERATOR_TIMEOUT = PREFIX + "artifact.iterator.timeout";
  public static final long DEFAULT_ARTIFACT_ITERATOR_TIMEOUT = TimeUtil.HOUR;
  private long artifactIteratorTimeout = DEFAULT_ARTIFACT_ITERATOR_TIMEOUT;

  ////////////////////////////////////////////////////////////////////////////////
  // CONFIG //////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////

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
      artifactIteratorTimeout =
          newConfig.getTimeInterval(PARAM_ARTIFACT_ITERATOR_TIMEOUT,
              DEFAULT_ARTIFACT_ITERATOR_TIMEOUT);
      smallContentThreshold =
          newConfig.getLong(PARAM_SMALL_CONTENT_THRESHOLD,
              DEFAULT_SMALL_CONTENT_THRESHOLD);

      // The first time setConfig() is called, replace the temporary
      // iterator continuation maps
      if (artifactIterators == null) {
        artifactIterators =
            Collections.synchronizedMap(new PassiveExpiringMap<>(artifactIteratorTimeout));
      }

      if (iteratorMapTimerRequest != null) {
        TimerQueue.cancel(iteratorMapTimerRequest);
      }
      iteratorMapTimerRequest = TimerQueue.schedule(
          Deadline.in(30 * TimeUtil.MINUTE), 30 * TimeUtil.MINUTE,
          (cookie) -> timeoutIterators(artifactIterators), null);
    }
  }

  TimerQueue.Request iteratorMapTimerRequest;

  private void timeoutIterators(Map map) {
    // Call isEmpty() for effect - runs removeAllExpired()
    map.isEmpty();
  }

  ////////////////////////////////////////////////////////////////////////////////
  // REST ////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////

  /**
   * POST /artifacts:
   * Adds artifacts to the repository
   *
   * @param properties
   * @param payload
   * @param httpResponseHeader
   * @return a {@code ResponseEntity<Artifact>}.
   */
  @Override
  public ResponseEntity<Artifact> createArtifact(String properties,
                                                 MultipartFile payload,
                                                 String httpResponseHeader) {

    long start = System.currentTimeMillis();

    String parsedRequest = String.format(
        "properties: %s, payload: %s, httpResponseHeader: %s: requestUrl: %s",
        properties, payload, httpResponseHeader,
        ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      boolean asHttpResponse = !StringUtil.isNullString(httpResponseHeader);

      // Read artifact properties part
      ArtifactProperties props = objMapper.readValue(properties, ArtifactProperties.class);

      ArtifactIdentifier artifactId = ArtifactDataUtil.buildArtifactIdentifier(props);

      if (artifactId.getVersion() != null) {
        throw new LockssRestServiceException(HttpStatus.BAD_REQUEST,
            "Version property not allowed");
      }

      // Check URI
      validateUri(artifactId.getUri(), parsedRequest);

      // Construct ArtifactData from payload part
      ArtifactData ad = WarcArtifactDataUtil.fromResource(payload.getInputStream());

      // Set artifact identifier
      ad.setIdentifier(artifactId);

      ad.setContentLength(payload.getSize());

      // Set artifact data digest
      MessageDigest md =
          ((LockssMultipartHttpServletRequest.LockssMultipartFile)payload).getDigest();

      String contentDigest = String.format("%s:%s",
          md.getAlgorithm(), new String(Hex.encodeHex(md.digest())));

      ad.setContentDigest(contentDigest);

      // Set artifact collection date if provided
      if (props.getCollectionDate() != null) {
        ad.setCollectionDate(props.getCollectionDate());
      }

      if (asHttpResponse) {
        try {
          HttpResponse httpResponse = ArtifactDataUtil.getHttpResponseFromStream(
              IOUtils.toInputStream(httpResponseHeader, StandardCharsets.UTF_8));

          // Set HTTP status
          ad.setHttpStatus(httpResponse.getStatusLine());

          // Set HTTP headers
          ad.setHttpHeaders(ArtifactDataUtil.transformHeaderArrayToHttpHeaders(httpResponse.getAllHeaders()));
        } catch (HttpException e) {
          throw new HttpMessageNotReadableException("Error parsing HTTP response header part", e);
        }
      } else {
        // Set artifact's Content-Type to the Content-Type of the part
        ad.getHttpHeaders()
            .set(HttpHeaders.CONTENT_TYPE, payload.getContentType());
      }

      //// Add artifact to internal repository
      try {
        Artifact artifact = repo.addArtifact(ad);

        long end = System.currentTimeMillis();

        log.debug2("Added new artifact [uuid: {}, duration: {} ms, length: {}]",
            artifact.getUuid(),
            TimeUtil.timeIntervalToString(end - start),
            StringUtil.sizeToString(payload.getSize()));

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
      // This one would be thrown by ArtifactDataFactory.fromHttpResponseStream(InputStream) while
      // parsing HTTP request. Return a 400 Bad Request response.
      throw new HttpMessageNotReadableException("Could not read artifact data from content part", e);
    }
  }

  /**
   * DELETE /artifacts/{artifactid}:
   * Deletes an artifact from this repository.
   *
   * @param artifactid   A String with the Identifier of the artifact.
   * @param namespace A String with the namespace of the artifact.
   * @return a {@code ResponseEntity<Void>}.
   */
  @Override
  public ResponseEntity<Void> deleteArtifact(String artifactid, String namespace) {

    String parsedRequest = String.format(
        "namespace: %s, artifactid: %s, requestUrl: %s",
        namespace, artifactid, ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      // Remove the artifact from the artifact store and index
      String key = artifactKey(namespace, artifactid);
      repo.deleteArtifact(namespace, artifactid);
      sendCacheInvalidateArtifact(ArtifactCache.InvalidateOp.Delete, key);
      return new ResponseEntity<>(HttpStatus.OK);

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
   * GET /artifacts/{artifactid}:
   * Retrieves an artifact from the repository.
   *
   * @param artifactid     A String with the Identifier of the artifact.
   * @param namespace      A String with the namespace of the artifact.
   * @param includeContent A {@link Boolean} indicating whether the artifact content part should be included in the
   *                       multipart response.
   * @return a {@link ResponseEntity} containing a {@link MultipartResponse}.
   */
  @Override
  public ResponseEntity getArtifactDataByMultipart(String artifactid, String namespace, String includeContent) {

    String parsedRequest = String.format(
        "namespace: %s, artifactid: %s, includeContent: %s, requestUrl: %s",
        namespace, artifactid, includeContent, ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      log.debug2("Retrieving artifact [namespace: {}, artifactId: {}]", namespace, artifactid);

      // Retrieve the ArtifactData from the artifact store
      ArtifactData artifactData = repo.getArtifactData(namespace, artifactid);

      // Transform ArtifactData into multipart map
      MultiValueMap<String, Object> parts =
          ArtifactDataUtil.generateMultipartMapFromArtifactData(
              artifactData,
              LockssRepository.IncludeContent.valueOf(includeContent),
              smallContentThreshold);

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
          artifactid);

      log.warn(errorMessage, e);
      log.warn("Parsed request: {}", parsedRequest);

      throw new LockssRestServiceException(
          LockssRestHttpException.ServerErrorType.DATA_ERROR,
          HttpStatus.INTERNAL_SERVER_ERROR,
          errorMessage, e, parsedRequest);
    }
  }

  /**
   * GET /artifacts/{artifactid}/payload:
   * Retrieves an artifact from the repository.
   *
   * @param artifactId     A String with the Identifier of the artifact.
   * @param namespace      A String with the namespace of the artifact.
   * @param includeContentParam A {@link Boolean} indicating whether the artifact content part should be included in the
   *                       multipart response.
   * @return a {@link ResponseEntity} containing a {@link MultipartResponse}.
   */
  @Override
  public ResponseEntity<Resource> getArtifactDataByPayload(String artifactId, String namespace,
                                                           String includeContentParam) {

    LockssRepository.IncludeContent includeContent =
        LockssRepository.IncludeContent.valueOf(includeContentParam);

    String parsedRequest = String.format(
        "namespace: %s, artifactId: %s, includeContent: %s, requestUrl: %s",
        namespace, artifactId, includeContent, ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      ArtifactData ad = repo.getArtifactData(namespace, artifactId);

      HttpHeaders httpHeaders = ad.getHttpHeaders();
      HttpHeaders respHeaders = new HttpHeaders();

      // Selectively copy artifact headers into REST response
      if (httpHeaders.containsKey(HttpHeaders.CONTENT_TYPE)) {
        respHeaders.setContentType(httpHeaders.getContentType());
      }

      if (httpHeaders.containsKey(HttpHeaders.LAST_MODIFIED)) {
        respHeaders.setLastModified(httpHeaders.getLastModified());
      }

      respHeaders.setContentLength(ad.getContentLength());
      respHeaders.set(ArtifactConstants.ARTIFACT_DIGEST_KEY, ad.getContentDigest());

      respHeaders.set(ArtifactConstants.ARTIFACT_STORE_DATE_KEY,
          DateTimeFormatter.ISO_INSTANT
              .format(Instant.ofEpochMilli(ad.getStoreDate()).atZone(ZoneOffset.UTC)));

      if (includeContent == LockssRepository.IncludeContent.ALWAYS ||
         (includeContent == LockssRepository.IncludeContent.IF_SMALL &&
             ad.getContentLength() <= smallContentThreshold)) {

        respHeaders.set(ArtifactConstants.INCLUDES_CONTENT, "true");

        // Return full HTTP response
        InputStreamResource resource = new InputStreamResource(ad.getInputStream());
        return new ResponseEntity<Resource>(resource, respHeaders, HttpStatus.OK);
      } else {
        // Remember the actual Content-Length in another header then set Content-Length to zero
        respHeaders.set(ArtifactConstants.X_LOCKSS_CONTENT_LENGTH,
            String.valueOf(respHeaders.getContentLength()));
        respHeaders.setContentLength(0);

        respHeaders.set(ArtifactConstants.INCLUDES_CONTENT, "false");

        // Return a response with HTTP status line and headers only
        return new ResponseEntity<Resource>(respHeaders, HttpStatus.OK);
      }
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
          artifactId);

      log.error(errorMessage, e);
      log.error("Parsed request: {}", parsedRequest);

      throw new LockssRestServiceException(
          LockssRestHttpException.ServerErrorType.DATA_ERROR,
          HttpStatus.INTERNAL_SERVER_ERROR,
          errorMessage, e, parsedRequest);
    }
  }

  /**
   * GET /artifacts/{artifactid}/response:
   * Retrieves an artifact from the repository.
   *
   * @param artifactId     A String with the Identifier of the artifact.
   * @param namespace      A String with the namespace of the artifact.
   * @param includeContentParam A {@link Boolean} indicating whether the artifact content part should be included in the
   *                       multipart response.
   * @return a {@link ResponseEntity} containing a {@link MultipartResponse}.
   */
  @Override
  public ResponseEntity<Resource> getArtifactDataByResponse(String artifactId, String namespace,
                                                            String includeContentParam) {

    LockssRepository.IncludeContent includeContent =
        LockssRepository.IncludeContent.valueOf(includeContentParam);

    String parsedRequest = String.format(
        "namespace: %s, artifactId: %s, includeContent: %s, requestUrl: %s",
        namespace, artifactId, includeContent, ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      ArtifactData ad = repo.getArtifactData(namespace, artifactId);

      boolean onlyHeaders = includeContent == LockssRepository.IncludeContent.NEVER ||
          (includeContent == LockssRepository.IncludeContent.IF_SMALL &&
              ad.getContentLength() > smallContentThreshold);

      InputStream httpResponseStream = onlyHeaders ?
            new ByteArrayInputStream(ArtifactDataUtil.getHttpResponseHeader(ad)) :
            ad.getResponseInputStream();

      InputStreamResource resource = new InputStreamResource(httpResponseStream);

      HttpHeaders restResponseHeaders = new HttpHeaders();
      restResponseHeaders.setContentType(APPLICATION_HTTP_RESPONSE);
      restResponseHeaders.set(ArtifactConstants.ARTIFACT_DATA_TYPE,
          ad.isHttpResponse() ? "response" : "resource");
      restResponseHeaders.set(ArtifactConstants.INCLUDES_CONTENT,
          String.valueOf(!onlyHeaders));

      restResponseHeaders.set(ArtifactConstants.ARTIFACT_STORE_DATE_KEY,
          DateTimeFormatter.ISO_INSTANT
              .format(Instant.ofEpochMilli(ad.getStoreDate()).atZone(ZoneOffset.UTC)));

      return new ResponseEntity<>(resource, restResponseHeaders, HttpStatus.OK);
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
          artifactId);

      log.error(errorMessage, e);
      log.error("Parsed request: {}", parsedRequest);

      throw new LockssRestServiceException(
          LockssRestHttpException.ServerErrorType.DATA_ERROR,
          HttpStatus.INTERNAL_SERVER_ERROR,
          errorMessage, e, parsedRequest);
    }
  }

  /**
   * GET /artifacts: Returns the committed artifacts of all versions
   * of a given URL, from a specified namespace.
   *
   * @param namespace          A String with the namespace of the artifact.
   * @param url                A String with the URL contained by the artifacts.
   * @param urlPrefix          A String with the prefix to be matched by the
   *                           artifact URLs.
   * @param limit              An Integer with the maximum number of artifacts
   *                           to be returned.
   * @param continuationToken  A String with the continuation token of the next
   *                           page of artifacts to be returned.
   * @return a {@code ResponseEntity<ArtifactPageInfo>} with the requested
   * artifacts.
   */
  @Override
  public ResponseEntity<ArtifactPageInfo> getArtifactsFromAllAus(String namespace,
                                                                 String url,
                                                                 String urlPrefix,
                                                                 String versions,
                                                                 Integer limit,
                                                                 String continuationToken) {

    String parsedRequest = String.format(
        "namespace: %s, url: %s, urlPrefix: %s, requestUrl: %s",
        namespace, url, urlPrefix, ServiceImplUtil.getFullRequestUrl(request));

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
      if (urlPrefix != null && url != null) {
        String errorMessage =
            "The 'urlPrefix' and 'url' arguments are mutually exclusive";

        log.warn(errorMessage);
        log.warn("Parsed request: {}", parsedRequest);

        throw new LockssRestServiceException(
            LockssRestHttpException.ServerErrorType.NONE, HttpStatus.BAD_REQUEST,
            errorMessage, parsedRequest);
      }

      List<Artifact> artifacts = new ArrayList<>();
      Iterator<Artifact> iterator = null;
      boolean missingIterator = false;
      ArtifactContinuationToken responseAct = null;

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

      if (iterator == null) {
        Iterable<Artifact> artifactIterable = null;
        ArtifactVersions artifactVersions = ArtifactVersions.valueOf(versions.toUpperCase());

        if (url != null) {
          artifactIterable = repo.getArtifactsWithUrlFromAllAus(namespace, url, artifactVersions);
        } else if (urlPrefix != null) {
          artifactIterable = repo.getArtifactsWithUrlPrefixFromAllAus(namespace, urlPrefix, artifactVersions);
        }

        if (artifactIterable != null) {
          // Yes: Get the iterator pointing to the first page of results.
          iterator = artifactIterable.iterator();

          // Check whether the artifacts provided in a previous response need to
          // be skipped.
          if (missingIterator) {
            // Yes: Initialize an artifact with properties from the last one
            // already returned in the previous page of results.
            Artifact lastArtifact = new Artifact();
            lastArtifact.setNamespace(requestAct.getNamespace());
            lastArtifact.setAuid(requestAct.getAuid());
            lastArtifact.setUri(requestAct.getUri());
            lastArtifact.setVersion(requestAct.getVersion());

            // Loop through the artifacts skipping those already returned
            // through a previous response.
            long skipStarted = TimeBase.nowMs();
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
            repo.incTimeSpentReiterating(TimeBase.msSince(skipStarted));
          }
        }
      }

      if (iterator != null) {
        // Populate the rest of the results for this response.
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
              lastArtifact.getNamespace(), lastArtifact.getAuid(),
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
      throw new LockssRestServiceException(
          LockssRestHttpException.ServerErrorType.DATA_ERROR, HttpStatus.INTERNAL_SERVER_ERROR,
          "IOException", e, parsedRequest);
    }
  }

  /**
   * PUT /artifacts/{artifactid}: Updates an artifact's properties
   * <p>
   * Currently limited to updating an artifact's committed status.
   *
   * @param artifactid   A String with the Identifier of the artifact.
   * @param committed    A Boolean with the artifact committed status.
   * @param namespace A String with the namespace of the artifact.
   * @return a {@code ResponseEntity<Artifact>}.
   */
  @Override
  public ResponseEntity updateArtifact(Boolean committed, String artifactid, String namespace) {
    String parsedRequest = String.format(
        "namespace: %s, artifactid: %s, committed: %s, requestUrl: %s",
        namespace, artifactid, committed, ServiceImplUtil.getFullRequestUrl(request));

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

      log.debug2("Committing artifact to permanent storage [artifactId: {}]", artifactid);

      // Commit the artifact
      Artifact updatedArtifact = repo.commitArtifact(namespace, artifactid);

      // Broadcast a cache invalidate signal for this artifact.
      // (Unless in bulk mode, where it takes noticeable time and is
      // unnecessary).
      if (!bulkAuids.contains(updatedArtifact.getAuid())) {
        sendCacheInvalidateArtifact(ArtifactCache.InvalidateOp.Commit,
                                    artifactKey(namespace, artifactid));
      }

      // Return the updated Artifact
      return new ResponseEntity<>(updatedArtifact, HttpStatus.OK);

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

  ////////////////////////////////////////////////////////////////////////////////
  // UTILITIES ///////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////

  String artifactKey(String namespace, String artifactUuid)
      throws IOException {
    Artifact art = repo.getArtifactFromUuid(artifactUuid);
    if (art != null) {
      return art.makeKey();
    } else {
      log.error("Expected artifact not found, can't send invalidate [uuid: {}]",
          artifactUuid);
      return null;
    }
  }

  protected void sendCacheInvalidateArtifact(ArtifactCache.InvalidateOp op,
                                             String key) {
    if (jmsProducer != null && key != null) {
      Map<String, Object> map = new HashMap<>();
      map.put(RestLockssRepository.REST_ARTIFACT_CACHE_MSG_ACTION,
          RestLockssRepository.REST_ARTIFACT_CACHE_MSG_ACTION_INVALIDATE_ARTIFACT);
      map.put(RestLockssRepository.REST_ARTIFACT_CACHE_MSG_OP, op.toString());
      map.put(RestLockssRepository.REST_ARTIFACT_CACHE_MSG_KEY, key);
      try {
        jmsProducer.sendMap(map);
      } catch (JMSException e) {
        log.error("Couldn't send cache invalidate notification", e);
      }
    }
  }

  private static Boolean isHttpResponseType(MediaType type) {
    return (APPLICATION_HTTP_RESPONSE.isCompatibleWith(type) && (type
        .getParameters().equals(APPLICATION_HTTP_RESPONSE.getParameters())));
  }

  private void validateUri(String uri, String parsedRequest) {
    log.debug2("uri = '{}'", uri);
    log.debug2("parsedRequest = '{}'", parsedRequest);
    if (StringUtil.isNullString(uri)) {
      String errorMessage = "The URI has not been provided";
      log.warn(errorMessage);
      log.warn("Parsed request: {}", parsedRequest);

      throw new LockssRestServiceException(
          LockssRestHttpException.ServerErrorType.NONE, HttpStatus.BAD_REQUEST,
          errorMessage, parsedRequest);
    }

    log.debug2("uri '{}' is valid.", uri);
  }

  ////////////////////////////////////////////////////////////////////////////////
  // JMS /////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////

  @jakarta.annotation.PostConstruct
  private void init() {
    setUpJms(JMS_BOTH,
        RestLockssRepository.REST_ARTIFACT_CACHE_ID,
        RestLockssRepository.REST_ARTIFACT_CACHE_TOPIC,
        new CacheInvalidateListener());
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
            case RestLockssRepository.REST_ARTIFACT_CACHE_MSG_ACTION_INVALIDATE_ARTIFACT:
            case RestLockssRepository.REST_ARTIFACT_CACHE_MSG_ACTION_INVALIDATE_AU:
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
