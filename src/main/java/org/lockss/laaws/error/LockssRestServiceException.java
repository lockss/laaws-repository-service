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

import java.time.LocalDateTime;
import org.springframework.http.HttpStatus;

/**
 * Unchecked exception to be thrown by REST service controllers and turned into
 * a meaningful error message by the standard exception handling.
 */
public class LockssRestServiceException extends RuntimeException {
  private static final long serialVersionUID = 4569911319063540372L;

  // The HTTP response status.
  private HttpStatus httpStatus;

  // The HTTP request parsed by the service. 
  private String parsedRequest = "";

  // The UTC date and time of the exception.
  private LocalDateTime utcTimestamp; 

  /**
   * Default constructor.
   */
  public LockssRestServiceException() {
      super();
  }

  /**
   * Constructor.
   *
   * @param message
   *          A String with the detail message.
   */
  public LockssRestServiceException(String message) {
      super(message);
  }

  /**
   * Constructor.
   *
   * @param message
   *          A String with the detail message.
   * @param cause
   *          A Throwable with the cause.
   */
  public LockssRestServiceException(String message, Throwable cause) {
      super(message, cause);
  }

  /**
   * Constructor.
   *
   * @param cause
   *          A Throwable with the cause.
   */
  public LockssRestServiceException(Throwable cause) {
      super(cause);
  }

  /**
   * Constructor.
   *
   * @param message
   *          A String with the detail message.
   * @param cause
   *          A Throwable with the cause.
   * @param enableSuppression
   *          A boolean indicating whether suppression is enabled.
   * @param writableStackTrace
   *          A boolean indicating whether the stack trace should be writable.
   */
  protected LockssRestServiceException(String message, Throwable cause,
                             boolean enableSuppression,
                             boolean writableStackTrace) {
      super(message, cause, enableSuppression, writableStackTrace);
  }

  /**
   * Constructor.
   *
   * @param message
   *          A String with the detail message.
   * @param httpStatus
   *          An HttpStatus with the HTTP response status.
   */
  public LockssRestServiceException(String message, HttpStatus httpStatus) {
    super(message);
    this.httpStatus = httpStatus;
  }

  /**
   * Constructor.
   *
   * @param message
   *          A String with the detail message.
   * @param httpStatus
   *          An HttpStatus with the HTTP response status.
   * @param parsedRequest
   *          A String with a copy of the parsed HTTP request contents.
   */
  public LockssRestServiceException(String message, HttpStatus httpStatus,
      String parsedRequest) {
    super(message);
    this.httpStatus = httpStatus;
    this.parsedRequest = parsedRequest;
  }

  /**
   * Constructor.
   *
   * @param message
   *          A String with the detail message.
   * @param httpStatus
   *          An HttpStatus with the HTTP response status.
   * @param parsedRequest
   *          A String with a copy of the parsed HTTP request contents.
   * @param utcTimestamp
   *          A LocalDateTime with the exception date and time.
   */
  public LockssRestServiceException(String message, HttpStatus httpStatus,
      String parsedRequest, LocalDateTime utcTimestamp) {
    super(message);
    this.httpStatus = httpStatus;
    this.parsedRequest = parsedRequest;
    this.utcTimestamp = utcTimestamp;
  }

  /**
   * Constructor.
   *
   * @param message
   *          A String with the detail message.
   * @param cause
   *          A Throwable with the cause.
   * @param httpStatus
   *          An HttpStatus with the HTTP response status.
   */
  public LockssRestServiceException(String message, Throwable cause,
      HttpStatus httpStatus) {
    super(message, cause);
    this.httpStatus = httpStatus;
  }

  /**
   * Constructor.
   *
   * @param message
   *          A String with the detail message.
   * @param cause
   *          A Throwable with the cause.
   * @param httpStatus
   *          An HttpStatus with the HTTP response status.
   * @param parsedRequest
   *          A String with a copy of the parsed HTTP request contents.
   */
  public LockssRestServiceException(String message, Throwable cause,
      HttpStatus httpStatus, String parsedRequest) {
    super(message, cause);
    this.httpStatus = httpStatus;
    this.parsedRequest = parsedRequest;
  }

  /**
   * Constructor.
   *
   * @param message
   *          A String with the detail message.
   * @param cause
   *          A Throwable with the cause.
   * @param httpStatus
   *          An HttpStatus with the HTTP response status.
   * @param parsedRequest
   *          A String with a copy of the parsed HTTP request contents.
   * @param utcTimestamp
   *          A LocalDateTime with the exception date and time.
   */
  public LockssRestServiceException(String message, Throwable cause,
      HttpStatus httpStatus, String parsedRequest, LocalDateTime utcTimestamp) {
    super(message, cause);
    this.httpStatus = httpStatus;
    this.parsedRequest = parsedRequest;
    this.utcTimestamp = utcTimestamp;
  }

  /**
   * Constructor.
   *
   * @param cause
   *          A Throwable with the cause.
   * @param httpStatus
   *          An HttpStatus with the HTTP response status.
   */
  public LockssRestServiceException(Throwable cause, HttpStatus httpStatus) {
    super(cause);
    this.httpStatus = httpStatus;
  }

  /**
   * Constructor.
   *
   * @param cause
   *          A Throwable with the cause.
   * @param httpStatus
   *          An HttpStatus with the HTTP response status.
   * @param parsedRequest
   *          A String with a copy of the parsed HTTP request contents.
   */
  public LockssRestServiceException(Throwable cause, HttpStatus httpStatus,
      String parsedRequest) {
    super(cause);
    this.httpStatus = httpStatus;
    this.parsedRequest = parsedRequest;
  }

  /**
   * Constructor.
   *
   * @param cause
   *          A Throwable with the cause.
   * @param httpStatus
   *          An HttpStatus with the HTTP response status.
   * @param parsedRequest
   *          A String with a copy of the parsed HTTP request contents.
   * @param utcTimestamp
   *          A LocalDateTime with the exception date and time.
   */
  public LockssRestServiceException(Throwable cause, HttpStatus httpStatus,
      String parsedRequest, LocalDateTime utcTimestamp) {
    super(cause);
    this.httpStatus = httpStatus;
    this.parsedRequest = parsedRequest;
    this.utcTimestamp = utcTimestamp;
  }

  /**
   * Constructor.
   *
   * @param message
   *          A String with the detail message.
   * @param cause
   *          A Throwable with the cause.
   * @param enableSuppression
   *          A boolean indicating whether suppression is enabled.
   * @param writableStackTrace
   *          A boolean indicating whether the stack trace should be writable.
   * @param httpStatus
   *          An HttpStatus with the HTTP response status.
   */
  protected LockssRestServiceException(String message, Throwable cause,
      boolean enableSuppression, boolean writableStackTrace,
      HttpStatus httpStatus) {
    super(message, cause, enableSuppression, writableStackTrace);
    this.httpStatus = httpStatus;
  }

  /**
   * Constructor.
   *
   * @param message
   *          A String with the detail message.
   * @param cause
   *          A Throwable with the cause.
   * @param enableSuppression
   *          A boolean indicating whether suppression is enabled.
   * @param writableStackTrace
   *          A boolean indicating whether the stack trace should be writable.
   * @param httpStatus
   *          An HttpStatus with the HTTP response status.
   * @param parsedRequest
   *          A String with a copy of the parsed HTTP request contents.
   */
  protected LockssRestServiceException(String message, Throwable cause,
      boolean enableSuppression, boolean writableStackTrace,
      HttpStatus httpStatus, String parsedRequest) {
    super(message, cause, enableSuppression, writableStackTrace);
    this.httpStatus = httpStatus;
    this.parsedRequest = parsedRequest;
  }

  /**
   * Constructor.
   *
   * @param message
   *          A String with the detail message.
   * @param cause
   *          A Throwable with the cause.
   * @param enableSuppression
   *          A boolean indicating whether suppression is enabled.
   * @param writableStackTrace
   *          A boolean indicating whether the stack trace should be writable.
   * @param httpStatus
   *          An HttpStatus with the HTTP response status.
   * @param parsedRequest
   *          A String with a copy of the parsed HTTP request contents.
   * @param utcTimestamp
   *          A LocalDateTime with the exception date and time.
   */
  protected LockssRestServiceException(String message, Throwable cause,
      boolean enableSuppression, boolean writableStackTrace,
      HttpStatus httpStatus, String parsedRequest, LocalDateTime utcTimestamp) {
    super(message, cause, enableSuppression, writableStackTrace);
    this.httpStatus = httpStatus;
    this.parsedRequest = parsedRequest;
    this.utcTimestamp = utcTimestamp;
  }

  /**
   * Provides the HTTP response status.
   * 
   * @return an HttpStatus with the HTTP response status.
   */
  public HttpStatus getHttpStatus() {
    return httpStatus;
  }

  /**
   * Sets the HTTP response status.
   * 
   * @param httpStatus
   *          An HttpStatus with the HTTP response status.
   * @return a LockssRestServiceException with this object.
   */
  public LockssRestServiceException setHttpStatus(HttpStatus httpStatus) {
    this.httpStatus = httpStatus;
    return this;
  }

  /**
   * Provides a copy of the parsed HTTP request contents.
   * 
   * @return a String with a copy of the parsed HTTP request contents.
   */
  public String getParsedRequest() {
    return parsedRequest;
  }

  /**
   * Sets the copy of the parsed HTTP request contents.
   * 
   * @param parsedRequest
   *          A String with a copy of the parsed HTTP request contents.
   * @return a LockssRestServiceException with this object.
   */
  public LockssRestServiceException setParsedRequest(String parsedRequest) {
    this.parsedRequest = parsedRequest;
    return this;
  }

  /**
   * Provides the exception date and time.
   * 
   * @return a LocalDateTime with the exception date and time.
   */
  public LocalDateTime getUtcTimestamp() {
    return utcTimestamp;
  }

  /**
   * Sets the exception date and time.
   * 
   * @param utcTimestamp
   *          A LocalDateTime with the exception date and time.
   * @return a LockssRestServiceException with this object.
   */
  public LockssRestServiceException setUtcTimestamp(LocalDateTime utcTimestamp)
  {
    this.utcTimestamp = utcTimestamp;
    return this;
  }
}
