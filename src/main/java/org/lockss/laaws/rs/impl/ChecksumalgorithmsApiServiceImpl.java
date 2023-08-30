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
import org.lockss.laaws.rs.api.ChecksumalgorithmsApiDelegate;
import org.lockss.log.L4JLogger;
import org.lockss.spring.base.BaseSpringApiServiceImpl;
import org.lockss.spring.error.LockssRestServiceException;
import org.lockss.util.StringUtil;
import org.lockss.util.rest.repo.LockssRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.security.Provider;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Service for accessing the supported checksum algorithms.
 */
@Service
public class ChecksumalgorithmsApiServiceImpl extends BaseSpringApiServiceImpl
    implements ChecksumalgorithmsApiDelegate {

  private static L4JLogger log = L4JLogger.getLogger();

  @Autowired
  LockssRepository repo;

  private final ObjectMapper objectMapper;
  private final HttpServletRequest request;

  private List<String> supportedMessageDigestAlgorithms = null;

  /**
   * Constructor for autowiring.
   * 
   * @param objectMapper
   *          An ObjectMapper for JSON processing.
   * @param request
   *          An HttpServletRequest with the HTTP request.
   */
  @org.springframework.beans.factory.annotation.Autowired
  public ChecksumalgorithmsApiServiceImpl(ObjectMapper objectMapper,
      HttpServletRequest request) {
    this.objectMapper = objectMapper;
    this.request = request;
  }

  /**
   * GET /checksumalgorithms:
   * Provides a list of checksum algorithms supported by this repository.
   *
   * @return a List<String> with the algorithm names.
   */
  @Override
  public ResponseEntity<List<String>> getSupportedChecksumAlgorithms() {
    String parsedRequest = String.format("requestUrl: %s",
	ServiceImplUtil.getFullRequestUrl(request));
    log.debug2("Parsed request: {}", parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      List<String> result = getSupportedMessageDigestAlgorithms();
      log.debug2("result = {}", result);
      return new ResponseEntity<>(result, HttpStatus.OK);
    } catch (Exception e) {
      String errorMessage =
	  "Exception caught trying to enumerate supported checksum algorithms";

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

  /**
   * Provides the names of the supported checksum algorithms.
   * 
   * @return a List<String> with the names of the supported checksum algorithms.
   */
  private List<String> getSupportedMessageDigestAlgorithms() {
    if (supportedMessageDigestAlgorithms == null) {
      supportedMessageDigestAlgorithms = new ArrayList<String>();

      for (Provider provider : Security.getProviders()) {
	log.trace("provider = {}", provider);

        for (Provider.Service service : provider.getServices()) {
          log.trace("service = {}", service);

          if ("MessageDigest".equals(service.getType())) {
            supportedMessageDigestAlgorithms.add(service.getAlgorithm());
            log.trace("algorithm = {}", service.getAlgorithm());

            String displayService = service.toString();
            int beginIndex =
        	displayService.indexOf("aliases: [") + "aliases: [".length();

            if (beginIndex >= "aliases: [".length()) {
              int endIndex = displayService.indexOf("]", beginIndex);
              String aliases = displayService.substring(beginIndex, endIndex);

              for (String alias : StringUtil.breakAt(aliases, ",")) {
                supportedMessageDigestAlgorithms.add(alias.trim());
                log.trace("alias = {}", alias.trim());
              }
            }
          }
        }
      }

      log.trace("supportedMessageDigestAlgorithms = {}",
	  supportedMessageDigestAlgorithms);
    }

    return supportedMessageDigestAlgorithms;
  }
}
