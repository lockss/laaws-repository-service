/*

Copyright (c) 2000-2020 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */
package org.lockss.laaws.rs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import org.lockss.laaws.error.LockssRestServiceException;
import org.lockss.laaws.rs.api.RepositoriesApiDelegate;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.log.L4JLogger;
import org.lockss.spring.base.BaseSpringApiServiceImpl;
import org.lockss.ws.entities.RepositoryWsResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

/**
 * Service for querying the properties of repositories.
 */
@Service
public class RepositoriesApiServiceImpl extends BaseSpringApiServiceImpl
    implements RepositoriesApiDelegate {

  private static L4JLogger log = L4JLogger.getLogger();

  @Autowired
  LockssRepository repo;

  private final ObjectMapper objectMapper;
  private final HttpServletRequest request;

  /**
   * Constructor for autowiring.
   * 
   * @param objectMapper
   *          An ObjectMapper for JSON processing.
   * @param request
   *          An HttpServletRequest with the HTTP request.
   */
  @org.springframework.beans.factory.annotation.Autowired
  public RepositoriesApiServiceImpl(ObjectMapper objectMapper,
      HttpServletRequest request) {
    this.objectMapper = objectMapper;
    this.request = request;
  }

  /**
   * GET /repositories?query={repositoryQuery}: Provides the selected properties
   * of selected repositories in the system.
   *
   * @param repositoryQuery A String with the
   *                        <a href="package-summary.html#SQL-Like_Query">SQL-like
   *                        query</a> used to specify what properties to
   *                        retrieve from which repository.
   * @return a {@code List<RepositoryWsResult>} with the results.
   */
  @Override
  public ResponseEntity<List<RepositoryWsResult>> getRepositories(
      String repositoryQuery) {
    String parsedRequest = String.format("repositoryQuery: %s, requestUrl: %s",
	repositoryQuery, ServiceImplUtil.getFullRequestUrl(request));
    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      // TODO: REPLACE THIS BLOCK WITH THE ACTUAL IMPLEMENTATION.
      List<RepositoryWsResult> result = null;
      // TODO: END OF BLOCK TO BE REPLACED.

      log.debug2("result = {}", result);
      return new ResponseEntity<>(result, HttpStatus.OK);
    } catch (Exception e) {
      String errorMessage = "Exception caught trying to import a pushed file.";
      log.warn(errorMessage, e);
      log.warn("Parsed request: {}", parsedRequest);

      throw new LockssRestServiceException(HttpStatus.INTERNAL_SERVER_ERROR,
	  errorMessage, e, parsedRequest);
    }
  }

  @Override
  public Optional<ObjectMapper> getObjectMapper() {
    return Optional.ofNullable(objectMapper);
  }
}
