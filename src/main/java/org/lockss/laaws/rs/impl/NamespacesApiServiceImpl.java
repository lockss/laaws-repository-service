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
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.List;

@Service
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
   * GET /namespaces:
   * Returns a list of namespaces managed by this repository.
   *
   * @return a List<String> with the namespaces.
   */
  @Override
  public ResponseEntity<List<String>> getNamespaces() {
    String parsedRequest = String.format("requestUrl: %s",
        ServiceImplUtil.getFullRequestUrl(request));

    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      List<String> namespaces = IterableUtils.toList(repo.getNamespaces());
      log.debug2("namespaces = {}", namespaces);
      return new ResponseEntity<>(namespaces, HttpStatus.OK);
    } catch (IOException e) {
      String errorMessage = "Could not enumerate namespaces";

      log.warn(errorMessage, e);
      log.warn("Parsed request: {}", parsedRequest);

      throw new LockssRestServiceException(
          LockssRestHttpException.ServerErrorType.DATA_ERROR,
          HttpStatus.INTERNAL_SERVER_ERROR,
          errorMessage, e, parsedRequest);
    }
  }
}
