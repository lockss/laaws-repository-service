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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * The body of a REST error response conforming to the media type
 * {@code application/vnd.error}.
 */
// The root element name as per the {@code application/vnd.error} specification.
@XmlRootElement(name = "errors")
public class RestResponseErrorBody
implements Iterable<RestResponseErrorBody.Error> {

  // The name of individual errors.
  @XmlElement(name = "error")
  private final List<Error> errors;

  /**
   * Protected default constructor to allow JAXB marshalling.
   */
  protected RestResponseErrorBody() {
    this.errors = new ArrayList<Error>();
  }

  /**
   * Constructor.
   */
  public RestResponseErrorBody(String message, String parsedRequest) {
    this(new Error(message, parsedRequest));
  }

  /**
   * Constructor.
   */
  public RestResponseErrorBody(String message, String parsedRequest,
      LocalDateTime utcTimestamp) {
    this(new Error(message, parsedRequest, utcTimestamp));
  }

  /**
   * Constructor.
   */
  public RestResponseErrorBody(Error error) {

    if (error == null) {
      throw new IllegalArgumentException("Error must not be null");
    }

    this.errors = new ArrayList<Error>();
    this.errors.add(error);
  }

  /**
   * Constructor.
   */
  public RestResponseErrorBody(LockssRestServiceException exception) {
    this(new Error(exception.getMessage(), exception.getParsedRequest(),
	exception.getUtcTimestamp()));
  }

  /**
   */
  public RestResponseErrorBody add(Error error) {
    this.errors.add(error);
    return this;
  }

  /**
   * Dummy method to allow JsonValue to be configured.
   */
  @JsonValue
  private List<Error> getErrors() {
    return errors;
  }

  @Override
  public Iterator<RestResponseErrorBody.Error> iterator() {
    return this.errors.iterator();
  }

  @Override
  public String toString() {
    return String.format("Errors[%s]", errors);
  }

  @Override
  public int hashCode() {
    return errors.hashCode();
  }

  @Override
  public boolean equals(Object obj) {

    if (this == obj) {
      return true;
    }

    if (!(obj instanceof RestResponseErrorBody)) {
      return false;
    }

    RestResponseErrorBody other = (RestResponseErrorBody) obj;
    return this.errors.equals(other.errors);
  }

  /**
   * A single error.
   */
  @XmlType
  public static class Error {

    @XmlElement @JsonProperty private final String message;
    @XmlElement @JsonProperty private final String parsedRequest;
    @XmlElement @JsonProperty private LocalDateTime utcTimestamp =
	LocalDateTime.now(Clock.systemUTC()); 

    /**
     * Protected default constructor to allow JAXB marshalling.
     */
    protected Error() {
      this.message = null;
      this.parsedRequest = null;
    }

    /**
     * Constructor.
     */
    public Error(String message, String parsedRequest) {

      if (message == null || message.trim().isEmpty()) {
	throw new IllegalArgumentException("message must not be null or empty");
      }

      if (parsedRequest == null) {
	throw new IllegalArgumentException("parsedRequest must not be null");
      }

      this.message = message;
      this.parsedRequest = parsedRequest;
    }

    /**
     * Constructor.
     */
    public Error(String message, String parsedRequest,
	LocalDateTime utcTimestamp) {

      if (message == null || message.trim().isEmpty()) {
	throw new IllegalArgumentException("message must not be null or empty");
      }

      if (parsedRequest == null) {
	throw new IllegalArgumentException("parsedRequest must not be null");
      }

      this.message = message;
      this.parsedRequest = parsedRequest;

      if (utcTimestamp != null) {
	this.utcTimestamp = utcTimestamp;
      }
    }

    public String getMessage() {
      return message;
    }

    public String getParsedRequest() {
      return parsedRequest;
    }

    public LocalDateTime getUtcTimestamp() {
      return utcTimestamp;
    }

    @Override
    public String toString() {
      return String.format("[Error message: %s, parsedRequest: %s, "
	  + "utcTimestamp: %s]", message, parsedRequest, utcTimestamp);
    }

    @Override
    public int hashCode() {

      int result = 17;

      result += 31 * message.hashCode();
      result += 31 * parsedRequest.hashCode();
      result += 31 * utcTimestamp.hashCode();

      return result;
    }

    @Override
    public boolean equals(Object obj) {

      if (obj == this) {
	return true;
      }

      if (!(obj instanceof Error)) {
	return false;
      }

      Error other = (Error)obj;

      return this.message.equals(other.message)
	  && this.parsedRequest.equals(other.parsedRequest)
	  && this.utcTimestamp.equals(other.utcTimestamp);
    }
  }
}
