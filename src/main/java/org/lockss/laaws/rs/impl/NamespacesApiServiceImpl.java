package org.lockss.laaws.rs.impl;

import org.apache.commons.collections4.IterableUtils;
import org.lockss.laaws.rs.api.NamespacesApiDelegate;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.log.L4JLogger;
import org.lockss.spring.base.BaseSpringApiServiceImpl;
import org.lockss.spring.error.LockssRestServiceException;
import org.lockss.util.rest.exception.LockssRestHttpException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.List;

public class NamespacesApiServiceImpl extends BaseSpringApiServiceImpl implements NamespacesApiDelegate {
  private static L4JLogger log = L4JLogger.getLogger();

  @Autowired
  LockssRepository repo;

  private final HttpServletRequest request;

  @Autowired
  public NamespacesApiServiceImpl(HttpServletRequest request) {
    this.request = request;
  }

  /**
   * GET /collections:
   * Returns a list of collection names managed by this repository.
   *
   * @return a List<String> with the collection names.
   */
  @Override
  public ResponseEntity<List<String>> getNamespaces() {
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
}
