package org.lockss.laaws.rs.impl;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.lockss.config.Configuration;
import org.lockss.laaws.rs.api.AusApiDelegate;
import org.lockss.util.rest.repo.model.BulkAuOpEnum;
import org.lockss.log.L4JLogger;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.rs.io.index.DispatchingArtifactIndex;
import org.lockss.spring.auth.AuthUtil;
import org.lockss.spring.auth.Roles;
import org.lockss.spring.base.BaseSpringApiServiceImpl;
import org.lockss.spring.base.LockssConfigurableService;
import org.lockss.spring.error.LockssRestServiceException;
import org.lockss.util.TimerQueue;
import org.lockss.util.UrlUtil;
import org.lockss.util.rest.exception.LockssRestHttpException;
import org.lockss.util.rest.repo.LockssRepository;
import org.lockss.util.rest.repo.model.*;
import org.lockss.util.time.Deadline;
import org.lockss.util.time.TimeUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import static org.lockss.laaws.rs.impl.ServiceImplUtil.validateLimit;

@Service
public class AusApiServiceImpl extends BaseSpringApiServiceImpl implements AusApiDelegate, LockssConfigurableService {
  private static L4JLogger log = L4JLogger.getLogger();

  @Autowired
  LockssRepository repo;

  private final HttpServletRequest request;

  private Set<String> bulkAuids = new CopyOnWriteArraySet<>();

  // The AUID iterators used in pagination:
  // This map is initialized to a normal map just in case it's
  // accessed before setConfig() is called and creates the official
  // PassiveExpiringMap.  I don't think the service methods here can
  // be called before the config is loaded, but this is easy insurance
  // that nothing seriously bad happens if they are.
  private Map<String, Iterator<String>> auidIterators =
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
      // iterator continuation map
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
      AuthUtil.checkHasRole(Roles.ROLE_CONTENT_ACCESS, Roles.ROLE_AU_ADMIN);

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
    AuthUtil.checkHasRole(Roles.ROLE_CONTENT_ACCESS, Roles.ROLE_AU_ADMIN);

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

      // Get the iterator ID (if any) used to provide a previous page
      // of results.
      String iteratorId = requestAct.getIteratorId();

      // Check whether this request is for the first page.
      if (iteratorId == null) {
        // Yes: Get the iterator pointing to first page of results.
        iterator = repo.getAuIds(namespace).iterator();

      } else {
        // No: Get the iterator (if any) used to provide a previous page of
        // results.
        iterator = auidIterators.remove(iteratorId);

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
        // Only generate a new UUID if we don't already have one (new iterator)
        if (iteratorId == null) {
          iteratorId = UUID.randomUUID().toString();
        }
        auidIterators.put(iteratorId, iterator);

        // Create the response continuation token.
        responseAct = new AuidContinuationToken(auids.get(auids.size() - 1),
            iteratorId);
        log.trace("responseAct = {}", responseAct);
      }

      log.trace("auids.size() = {}", auids.size());

      PageInfo pageInfo = new PageInfo();
      pageInfo.setItemsInPage(auids.size());

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
  public ResponseEntity<Void> handleBulkAuOp(String auid, BulkAuOpEnum op, String namespace) {

    String parsedRequest = String.format("namespace: %s, auid: %s, op: %s, requestUrl: %s",
        namespace, auid, op, ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);
    AuthUtil.checkHasRole(Roles.ROLE_AU_ADMIN);

    if (bulkIndexEnabled) {
      ArtifactIndex index = ((BaseLockssRepository)repo).getArtifactIndex();
      try {
        switch (op) {
          case START:
            log.debug("startBulkStore({}, {})", namespace, auid);
            bulkAuids.add(auid);
            index.startBulkStore(namespace, auid);
            break;

          case FINISH:
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
