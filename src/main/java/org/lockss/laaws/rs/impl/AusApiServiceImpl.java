package org.lockss.laaws.rs.impl;

import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.lockss.config.Configuration;
import org.lockss.laaws.rs.api.AusApiDelegate;
import org.lockss.log.L4JLogger;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.rs.io.index.DispatchingArtifactIndex;
import org.lockss.spring.base.BaseSpringApiServiceImpl;
import org.lockss.spring.base.LockssConfigurableService;
import org.lockss.spring.error.LockssRestServiceException;
import org.lockss.util.TimerQueue;
import org.lockss.util.UrlUtil;
import org.lockss.util.rest.exception.LockssRestHttpException;
import org.lockss.util.rest.repo.LockssRepository;
import org.lockss.util.rest.repo.model.*;
import org.lockss.util.rest.repo.util.ArtifactComparators;
import org.lockss.util.time.Deadline;
import org.lockss.util.time.TimeUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import static org.lockss.laaws.rs.impl.ServiceImplUtil.populateArtifacts;
import static org.lockss.laaws.rs.impl.ServiceImplUtil.validateLimit;

@Service
public class AusApiServiceImpl extends BaseSpringApiServiceImpl implements AusApiDelegate, LockssConfigurableService {
  private static L4JLogger log = L4JLogger.getLogger();

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

  // The auid iterators used in pagination.
  private Map<Integer, Iterator<String>> auidIterators =
      new ConcurrentHashMap<>();

  @Autowired
  public AusApiServiceImpl(HttpServletRequest request) {
    this.request = request;
  }

  ////////////////////////////////////////////////////////////////////////////////
  // PARAMS //////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////

  public static final String PREFIX = "org.lockss.repository.";

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
   * Default number of AUIDs that will be returned in a single (paged)
   * response
   */
  public static final String PARAM_DEFAULT_AUID_PAGESIZE = PREFIX + "auid.pagesize.default";
  public static final int DEFAULT_DEFAULT_AUID_PAGESIZE = 1000;
  private int defaultAuidPageSize = DEFAULT_DEFAULT_AUID_PAGESIZE;

  /**
   * Max number of AUIDs that will be returned in a single (paged)
   * response
   */
  public static final String PARAM_MAX_AUID_PAGESIZE = PREFIX + "auid.pagesize.max";
  public static final int DEFAULT_MAX_AUID_PAGESIZE = 2000;
  private int maxAuidPageSize = DEFAULT_MAX_AUID_PAGESIZE;

  /**
   * Batch size when adding Artifacts in bulk, when using a {@link DispatchingArtifactIndex}.
   */
  public static final String PARAM_BULK_INDEX_BATCH_SIZE = PREFIX + "bulkIndexBatchSize";
  public static final int DEFAULT_BULK_INDEX_BATCH_SIZE = 1000;
  private int bulkIndexBatchSize = DEFAULT_BULK_INDEX_BATCH_SIZE;

  /**
   * Set false to disable putting AUs into bulk mode
   */
  public static final String PARAM_BULK_INDEX_ENABLED =
    PREFIX + "bulkIndexEnabled";
  public static final boolean DEFAULT_BULK_INDEX_ENABLED = true;
  private boolean bulkIndexEnabled = DEFAULT_BULK_INDEX_ENABLED;

  /**
   * Interval after which unused Artifact iterator continuations will
   * be discarded.  Change requires restart to take effect.
   */
  public static final String PARAM_ARTIFACT_ITERATOR_TIMEOUT = PREFIX + "artifact.iterator.timeout";
  public static final long DEFAULT_ARTIFACT_ITERATOR_TIMEOUT = 48 * TimeUtil.HOUR;
  private long artifactIteratorTimeout = DEFAULT_ARTIFACT_ITERATOR_TIMEOUT;

  /**
   * Interval after which unused AUID iterator continuations will
   * be discarded.  Change requires restart to take effect.
   */
  public static final String PARAM_AUID_ITERATOR_TIMEOUT = PREFIX + "auid.iterator.timeout";
  public static final long DEFAULT_AUID_ITERATOR_TIMEOUT = 48 * TimeUtil.HOUR;
  private long auidIteratorTimeout = DEFAULT_AUID_ITERATOR_TIMEOUT;

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
      defaultAuidPageSize = newConfig.getInt(PARAM_DEFAULT_AUID_PAGESIZE,
          DEFAULT_DEFAULT_AUID_PAGESIZE);
      maxAuidPageSize = newConfig.getInt(PARAM_MAX_AUID_PAGESIZE,
          DEFAULT_MAX_AUID_PAGESIZE);
      bulkIndexBatchSize =
          newConfig.getInt(PARAM_BULK_INDEX_BATCH_SIZE,
              DEFAULT_BULK_INDEX_BATCH_SIZE);
      bulkIndexEnabled = newConfig.getBoolean(PARAM_BULK_INDEX_ENABLED,
                                              DEFAULT_BULK_INDEX_ENABLED);

      // The first time setConfig() is called, replace the temporary
      // iterator continuation maps
      if (!(artifactIterators instanceof PassiveExpiringMap)) {
        artifactIterators =
            Collections.synchronizedMap(new PassiveExpiringMap<>(artifactIteratorTimeout));
      }
      if (!(auidIterators instanceof PassiveExpiringMap)) {
        auidIterators =
            Collections.synchronizedMap(new PassiveExpiringMap<>(auidIteratorTimeout));
      }

      if (iteratorMapTimer != null) {
        TimerQueue.cancel(iteratorMapTimer);
      }
      iteratorMapTimer = TimerQueue.schedule(Deadline.in(
          1 * TimeUtil.HOUR), 1 * TimeUtil.HOUR, iteratorMapTimeout, null);
    }
  }

  TimerQueue.Request iteratorMapTimer;

  // Timer callback for periodic removal of timed-out iterator continuations
  private TimerQueue.Callback iteratorMapTimeout =
      new TimerQueue.Callback() {
        public void timerExpired(Object cookie) {
          timeoutIterators(artifactIterators);
          timeoutIterators(auidIterators);
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
   * GET /aus/{auid}/artifacts:
   * Get a list with all the artifacts in a namespace and Archival Unit or a pageful of the list
   * defined by the continuation token and size.
   *
   * @param auid               A String with the Archival Unit ID (AUID) of
   *                           artifact.
   * @param namespace          A String with the namespace of the artifact.
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
  public ResponseEntity<ArtifactPageInfo> getArtifacts(String auid, String namespace, String url, String urlPrefix,
                                                       String version, Boolean includeUncommitted, Integer limit, String continuationToken) {

    String parsedRequest = String.format("namespace: %s, auid: %s, url: %s, "
            + "urlPrefix: %s, version: %s, includeUncommitted: %s, limit: %s, "
            + "continuationToken: %s, requestUrl: %s",
        namespace, auid, url, urlPrefix, version, includeUncommitted, limit,
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
          version == null || version.equalsIgnoreCase("latest");
      log.trace("isLatestVersion = {}", isLatestVersion);

      boolean isAllVersions =
          version != null && version.equalsIgnoreCase("all");
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
              "The 'version' argument is invalid";

          log.warn(errorMessage);
          log.warn("Parsed request: {}", parsedRequest);

          throw new LockssRestServiceException(
              LockssRestHttpException.ServerErrorType.NONE, HttpStatus.BAD_REQUEST,
              errorMessage, parsedRequest);
        }
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

      if (isAllUrls && isAllVersions) {
        log.trace("All versions of all URLs");
        if (iterator == null) {
          artifactIterable = repo.getArtifactsAllVersions(namespace, auid);
        }
      } else if (urlPrefix != null && isAllVersions) {
        log.trace("All versions of all URLs matching a prefix");
        if (iterator == null) {
          artifactIterable = repo.getArtifactsWithPrefixAllVersions(
              namespace, auid, urlPrefix);
        }
      } else if (url != null && isAllVersions) {
        log.trace("All versions of a URL");
        if (iterator == null) {
          artifactIterable =
              repo.getArtifactsAllVersions(namespace, auid, url);
        }
      } else if (isAllUrls && isLatestVersion) {
        log.trace("Latest versions of all URLs");
        if (iterator == null) {
          artifactIterable = repo.getArtifacts(namespace, auid);
        }
      } else if (urlPrefix != null && isLatestVersion) {
        log.trace("Latest versions of all URLs matching a prefix");
        if (iterator == null) {
          artifactIterable =
              repo.getArtifactsWithPrefix(namespace, auid, urlPrefix);
        }
      } else if (url != null && isLatestVersion) {
        log.trace("Latest version of a URL");
        Artifact artifact = repo.getArtifact(namespace, auid, url);
        log.trace("artifact = {}", artifact);

        if (artifact != null) {
          artifacts.add(artifact);
        }
      } else if (url != null && numericVersion > 0) {
        log.trace("Given version of a URL");
        log.trace("namespace = {}", namespace);
        log.trace("auid = {}", auid);
        log.trace("url = {}", url);
        log.trace("numericVersion = {}", numericVersion);
        log.trace("includeUncommittedValue = {}", includeUncommittedValue);
        Artifact artifact = repo.getArtifactVersion(namespace, auid, url,
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

        nextLinkBuffer.append("&namespace=").append(UrlUtil.encodeUrl(namespace));

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
   * GET /aus/{auid}/size:
   * Get the size of Archival Unit artifacts in a namespace.
   *
   * @param auid         A String with the Archival Unit ID (AUID).
   * @param namespace    A String with the namespace of the Archival Unit.
   * @return a {@link ResponseEntity< AuSize >}.
   */
  @Override
  public ResponseEntity<AuSize> getArtifactsSize(String auid, String namespace) {
    String parsedRequest = String.format("namespace: %s, auid: %s, requestUrl: %s",
        namespace, auid, ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    try {
      // Validate request
      ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

      // Get and return AU size from internal LOCKSS repository
      AuSize result = repo.auSize(namespace, auid);
      log.debug2("result = {}", result);
      return new ResponseEntity<AuSize>(result, HttpStatus.OK);
    } catch (IOException e) {
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
   * GET /aus:
   * Get all Archival Unit IDs (AUIDs) in a namespace or a pageful of the list
   * defined by the continuation token and size.
   *
   * @param namespace A String with the namespace of the Archival Units.
   * @param limit             An Integer with the maximum number of archival
   *                          unit identifiers to be returned.
   * @param continuationToken A String with the continuation token of the next
   *                          page of archival unit identifiers to be returned.
   * @return a {@code ResponseEntity<AuidPageInfo>}.
   */
  @Override
  public ResponseEntity<AuidPageInfo> getAus(String namespace, Integer limit,
                                             String continuationToken) {

    String parsedRequest = String.format("namespace: %s, requestUrl: %s",
        namespace, ServiceImplUtil.getFullRequestUrl(request));

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
      List<String> auids = new ArrayList<>();
      AuidContinuationToken responseAct = null;
      Iterator<String> iterator = null;

      // Get the iterator hash code (if any) used to provide a previous page
      // of results.
      Integer iteratorHashCode = requestAct.getIteratorHashCode();

      // Check whether this request is for the first page.
      if (iteratorHashCode == null) {
        // Yes: Get the iterator pointing to first page of results.
        iterator = repo.getAuIds(namespace).iterator();

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
          iterator = repo.getAuIds(namespace).iterator();

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

        nextLinkBuffer.append("&namespace=").append(UrlUtil.encodeUrl(namespace));

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

    } catch (IOException e) {
      String errorMessage =
          "Unexpected exception caught while attempting to get AU ids";

      log.warn(errorMessage, e);
      log.warn("Parsed request: {}", parsedRequest);

      throw new LockssRestServiceException(
//          LockssRestHttpException.ServerErrorType.DATA_ERROR,
          HttpStatus.INTERNAL_SERVER_ERROR,
          errorMessage, e, parsedRequest);
    }
  }

  /**
   * Handles bulk transfer operations for an AUID in a namespace. Possible operations are {@code start} and {@code
   * finish}.
   *
   * @param auid A {@link String} containing the AUID to operate on.
   * @param op A {@link String} with the operation to perform. Must be either {@code start} or {@code finish}.
   * @param namespace A {@link String} containing the namespace of the AUID to operate on.
   * @return TBD
   */
  @Override
  public ResponseEntity<Void> handleBulkAuOp(String auid, String op, String namespace) {

    String parsedRequest = String.format("namespace: %s, auid: %s, op: %s, requestUrl: %s",
        namespace, auid, op, ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    if (bulkIndexEnabled) {
      ArtifactIndex index = ((BaseLockssRepository)repo).getArtifactIndex();
      try {
        switch (op) {
          case "start":
            log.debug("startBulkStore({}, {})", namespace, auid);
            bulkAuids.add(auid);
            index.startBulkStore(namespace, auid);
            break;

          case "finish":
            log.debug("finishBulkStore({}, {})", namespace, auid);
            bulkAuids.remove(auid);
            index.finishBulkStore(namespace, auid, bulkIndexBatchSize);
            break;

          default:
            throw new LockssRestServiceException("Unknown bulk operation")
                .setServerErrorType(LockssRestHttpException.ServerErrorType.NONE)
                .setHttpStatus(HttpStatus.BAD_REQUEST)
                .setServletPath(request.getServletPath())
                .setParsedRequest(parsedRequest);
        }
      } catch (IOException e) {
        String errorMessage = String.format("IOException attempting to start or finish bulk store: %s", auid);
        log.warn(errorMessage, e);
        log.warn("Parsed request: {}", parsedRequest);

        throw new LockssRestServiceException(
            LockssRestHttpException.ServerErrorType.APPLICATION_ERROR,
            HttpStatus.INTERNAL_SERVER_ERROR,
            errorMessage, e, parsedRequest);
      }
    } else {
      log.debug2("Bulk indexing disabled, ignoring bulk {} for {}", op, auid);
    }
    return new ResponseEntity<>(HttpStatus.OK);
  }

  ////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////

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

}
