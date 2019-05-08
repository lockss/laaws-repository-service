/*
 * Copyright (c) 2018, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.laaws.error;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestMapping;

@RequestMapping(produces = "application/vnd.error+json")
public class SpringControllerAdvice {

  private static Logger log =
      LoggerFactory.getLogger(SpringControllerAdvice.class);

  /**
   * Handles a custom LOCKSS REST service exception.
   *
   * @param lrse
   *          A LockssRestServiceException with the details of the problem.
   * @return a ResponseEntity<RestResponseErrorBody> with the error response in
   *         JSON format with media type {@code application/vnd.error+json}.
   */
  @ExceptionHandler(LockssRestServiceException.class)
  public ResponseEntity<RestResponseErrorBody> handler(
      final LockssRestServiceException lrse) {
    return new ResponseEntity<>(new RestResponseErrorBody(lrse),
	lrse.getHttpStatus());
  }

  /**
   * Handles any other unhandled exception as a last resort.
   *
   * @param e
   *          An Exception with the exception not handled by other exception
   *          handler methods.
   * @return a ResponseEntity<RestResponseErrorBody> with the error response in
   *         JSON format with media type {@code application/vnd.error+json}.
   */
  @ExceptionHandler(Exception.class)
  public ResponseEntity<RestResponseErrorBody> defaultHandler(Exception e) {
    log.error("Caught otherwise unhandled exception", e);
    return new ResponseEntity<>(new RestResponseErrorBody(e.getMessage(),
	e.getClass().getSimpleName()), HttpStatus.INTERNAL_SERVER_ERROR);
  }
}
