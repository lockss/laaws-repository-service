package org.lockss.laaws.rs.impl;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.lockss.config.Configuration;
import org.lockss.laaws.rs.api.ArtifactsApiDelegate;
import org.lockss.laaws.rs.core.ArtifactCache;
import org.lockss.laaws.rs.core.LockssNoSuchArtifactIdException;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.core.RestLockssRepository;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.multipart.DigestFileItem;
import org.lockss.laaws.rs.util.*;
import org.lockss.log.L4JLogger;
import org.lockss.spring.base.BaseSpringApiServiceImpl;
import org.lockss.spring.base.LockssConfigurableService;
import org.lockss.spring.error.LockssRestServiceException;
import org.lockss.util.StringUtil;
import org.lockss.util.TimerQueue;
import org.lockss.util.UrlUtil;
import org.lockss.util.jms.JmsUtil;
import org.lockss.util.rest.exception.LockssRestHttpException;
import org.lockss.util.rest.multipart.MultipartResponse;
import org.lockss.util.time.Deadline;
import org.lockss.util.time.TimeUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.*;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.commons.CommonsMultipartFile;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import static org.lockss.laaws.rs.impl.ServiceImplUtil.populateArtifacts;
import static org.lockss.laaws.rs.impl.ServiceImplUtil.validateLimit;

public class ArtifactsApiServiceImpl extends BaseSpringApiServiceImpl
    implements ArtifactsApiDelegate, LockssConfigurableService {
  private static L4JLogger log = L4JLogger.getLogger();

  public static final String APPLICATION_HTTP_RESPONSE_VALUE =
      "application/http;msgtype=response";

  public static final MediaType APPLICATION_HTTP_RESPONSE =
      MediaType.parseMediaType(APPLICATION_HTTP_RESPONSE_VALUE);

  @Autowired
  LockssRepository repo;

  private final HttpServletRequest request;

  private Set<String> bulkAuids = new CopyOnWriteArraySet<>();

  // These maps are initialized to normal maps just in case they're
  // accessed before setConfig() is called and creates the official
  // PassiveExpiringMaps.  I don't think the service methods here can
  // be called before the config is loaded, but this is easy insurance
  // that nothing seriously bad happen if they are.

  // The artifact iterators used in pagination.
  private Map<Integer, Iterator<Artifact>> artifactIterators =
      new ConcurrentHashMap<>();

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
  public static final long DEFAULT_ARTIFACT_ITERATOR_TIMEOUT = 48 * TimeUtil.HOUR;
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
      if (!(artifactIterators instanceof PassiveExpiringMap)) {
        artifactIterators =
            Collections.synchronizedMap(new PassiveExpiringMap<>(artifactIteratorTimeout));
      }
      TimerQueue.schedule(Deadline.in(1 * TimeUtil.HOUR), 1 * TimeUtil.HOUR,
          iteratorMapTimeout, null);
    }
  }

  // Timer callback for periodic removal of timed-out iterator continuations
  private TimerQueue.Callback iteratorMapTimeout =
      new TimerQueue.Callback() {
        public void timerExpired(Object cookie) {
          timeoutIterators(artifactIterators);
        }
      };

  private void timeoutIterators(Map map) {
    // Call isEmpty() for effect - runs removeAllExpired()
    map.isEmpty();
  }

  ////////////////////////////////////////////////////////////////////////////////
  // REST ////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////

  /**
   * POST /collections/{collectionid}/artifacts:
   * Adds artifacts to the repository
   *
   * @param auid         A String with the Archival Unit ID (AUID) of new artifact.
   * @param uri          A String with the URI represented by this artifact.
   * @param content      A MultipartFile with the artifact content.
   * @param namespace A String with the name of the collection containing the artifact.
   * @param collectionDate A String with the name of the collection containing the artifact.
   * @return a {@code ResponseEntity<Artifact>}.
   */
  @Override
  public ResponseEntity<Artifact> createArtifact(String auid,
                                                 String uri,
                                                 MultipartFile content,
                                                 String namespace,
                                                 Long collectionDate) {

    long start = System.currentTimeMillis();

    String parsedRequest = String.format(
        "namespace: %s, auid: %s, uri: %s, collectionDate: %s, requestUrl: %s",
        namespace, auid, uri, collectionDate, ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

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
          new ArtifactIdentifier(namespace, auid, uri, 0);
      artifactData.setIdentifier(id);

      // Set artifact data content-length
      byte[] headers = ArtifactDataUtil.getHttpResponseHeader(artifactData);
      artifactData.setContentLength(content.getSize() - headers.length);

      // Set artifact data digest
      DigestFileItem item = ((DigestFileItem)((CommonsMultipartFile)content).getFileItem());

      String contentDigest = String.format("%s:%s",
          item.getDigest().getAlgorithm(),
          new String(Hex.encodeHex(item.getDigest().digest())));

      artifactData.setContentDigest(contentDigest);

      // Set artifact collection date if provided
      if (collectionDate != null) {
        artifactData.setCollectionDate(collectionDate);
      }

      //// Add artifact to internal repository
      try {
        Artifact artifact = repo.addArtifact(artifactData);
        log.debug2("Wrote artifact to {}", artifact.getStorageUrl());

        long end = System.currentTimeMillis();

        log.debug2("artifactId: {}, duration: {}, length: {}",
            artifact.getId(),
            TimeUtil.timeIntervalToString(end-start),
            StringUtil.sizeToString(content.getSize()));

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
   * DELETE /collections/{collectionid}/artifacts/{artifactid}:
   * Deletes an artifact from a collection managed by this repository.
   *
   * @param artifactid   A String with the Identifier of the artifact.
   * @param namespace A String with the name of the collection containing the artifact.
   * @return a {@code ResponseEntity<Void>}.
   */
  @Override
  public ResponseEntity<Void> deleteArtifact(String artifactid,
                                             String namespace) {
    String parsedRequest = String.format(
        "namespace: %s, artifactid: %s, requestUrl: %s",
        namespace, artifactid, ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      // Remove the artifact from the artifact store and index
      String key = artifactKey(namespace, artifactid);
      repo.deleteArtifact(namespace, artifactid);
      sendCacheInvalidate(ArtifactCache.InvalidateOp.Delete, key);
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
   * GET /collections/{collectionid}/artifacts/{artifactid}:
   * Retrieves an artifact from the repository.
   *
   * @param artifactid     A String with the Identifier of the artifact.
   * @param namespace      A String with the name of the collection containing the artifact.
   * @param includeContent A {@link Boolean} indicating whether the artifact content part should be included in the
   *                       multipart response.
   * @return a {@link ResponseEntity} containing a {@link MultipartResponse}.
   */
  @Override
  public ResponseEntity getArtifact(String artifactid, String namespace, String includeContent) {
    String parsedRequest = String.format(
        "namespace: %s, artifactid: %s, includeContent: %s, requestUrl: %s",
        namespace, artifactid, includeContent, ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      log.debug2("Retrieving artifact [namespace: {}, artifactId: {}]", namespace, artifactid);

      // Retrieve the ArtifactData from the artifact store
      ArtifactData artifactData = repo.getArtifactData(namespace, artifactid);

      // Break ArtifactData into multiparts
      MultiValueMap<String, Object> parts = generateMultipartResponseFromArtifactData(
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
   * GET /collections/{collectionid}/artifacts: Returns the committed artifacts of all versions
   * of a given URL, from a specified collection.
   *
   * @param namespace A String with the name of the collection
   *                           containing the artifact.
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

      ArtifactVersions artifactVersions = ArtifactVersions.valueOf(versions);

      if (url != null) {
        artifactIterable = repo.getArtifactsWithUrlFromAllAus(namespace, url, artifactVersions);
      } else if (urlPrefix != null) {
        artifactIterable = repo.getArtifactsWithUrlPrefixFromAllAus(namespace, urlPrefix, artifactVersions);
      }

      ArtifactContinuationToken responseAct = null;

      // Check whether an iterator is involved in obtaining the response.
      if (iterator != null || artifactIterable != null) {
        // Yes: Check whether a new iterator is needed.
        if (iterator == null) {
          // Yes: Get the iterator pointing to the first page of results.
          iterator = artifactIterable.iterator();

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
   * PUT /collections/{collectionid}/artifacts/{artifactid}:
   * Updates an artifact's properties
   * <p>
   * Currently limited to updating an artifact's committed status.
   *
   * @param artifactid   A String with the Identifier of the artifact.
   * @param committed    A Boolean with the artifact committed status.
   * @param namespace A String with the name of the collection containing the artifact.
   * @return a {@code ResponseEntity<Artifact>}.
   */
  @Override
  public ResponseEntity updateArtifact(String artifactid, Boolean committed, String namespace) {
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
        sendCacheInvalidate(ArtifactCache.InvalidateOp.Commit, artifactKey(namespace, artifactid));
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

  public static MultiValueMap<String, Object> generateMultipartResponseFromArtifactData(
      ArtifactData artifactData, LockssRepository.IncludeContent includeContent, long smallContentThreshold)
      throws IOException {

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
    if (ad.getArtifactState() != null) {
      headers.set(
          ArtifactConstants.ARTIFACT_STATE_COMMITTED,
          String.valueOf(ad.getArtifactState().isCommitted())
      );

      headers.set(
          ArtifactConstants.ARTIFACT_STATE_DELETED,
          String.valueOf(ad.getArtifactState().isDeleted())
      );
    }

    //// Unclassified artifact repository headers
    headers.set(ArtifactConstants.ARTIFACT_LENGTH_KEY, String.valueOf(ad.getContentLength()));
    headers.set(ArtifactConstants.ARTIFACT_DIGEST_KEY, ad.getContentDigest());

//    headers.set(ArtifactConstants.ARTIFACT_ORIGIN_KEY, ???);
//    headers.set(ArtifactConstants.ARTIFACT_COLLECTION_DATE, ???);

    return headers;
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

  private static Boolean isHttpResponseType(MediaType type) {
    return (APPLICATION_HTTP_RESPONSE.isCompatibleWith(type) && (type
        .getParameters().equals(APPLICATION_HTTP_RESPONSE.getParameters())));
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

  ////////////////////////////////////////////////////////////////////////////////
  // JMS /////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////

  @javax.annotation.PostConstruct
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
