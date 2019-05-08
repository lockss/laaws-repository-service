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
implements Iterable<RestResponseErrorBody.RestResponseError> {

  // The element name of individual errors.
  @XmlElement(name = "error")
  private final List<RestResponseError> errors;

  /**
   * Protected default constructor to allow JAXB marshalling.
   */
  protected RestResponseErrorBody() {
    this.errors = new ArrayList<RestResponseError>();
  }

  /**
   * Constructor.
   *
   * @param message
   *          A String with the detail message.
   * @param parsedRequest
   *          A String with a copy of the parsed HTTP request contents.
   */
  public RestResponseErrorBody(String message, String parsedRequest) {
    this(new RestResponseError(message, parsedRequest));
  }

  /**
   * Constructor.
   *
   * @param message
   *          A String with the detail message.
   * @param parsedRequest
   *          A String with a copy of the parsed HTTP request contents.
   * @param utcTimestamp
   *          A LocalDateTime with the exception date and time.
   */
  public RestResponseErrorBody(String message, String parsedRequest,
      LocalDateTime utcTimestamp) {
    this(new RestResponseError(message, parsedRequest, utcTimestamp));
  }

  /**
   * Constructor.
   *
   * @param error
   *          An Error to be included in the body.
   */
  public RestResponseErrorBody(RestResponseError error) {
    this.errors = new ArrayList<RestResponseError>();

    if (error != null) {
      this.errors.add(error);
    }
  }

  /**
   * Constructor.
   *
   * @param exception
   *          A LockssRestServiceException with the data to be included in the
   *          body.
   */
  public RestResponseErrorBody(LockssRestServiceException exception) {
    this(new RestResponseError(exception.getMessage(),
	exception.getParsedRequest(), exception.getUtcTimestamp()));
  }

  /**
   * Adds an error.
   *
   * @param error
   *          A RestResponseError with the error to be added.
   * @return a RestResponseErrorBody with this object.
   */
  public RestResponseErrorBody add(RestResponseError error) {
    this.errors.add(error);
    return this;
  }

  /**
   * Dummy method to allow JsonValue to be configured.
   */
  @JsonValue
  private List<RestResponseError> getErrors() {
    return errors;
  }

  @Override
  public Iterator<RestResponseErrorBody.RestResponseError> iterator() {
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
  public static class RestResponseError {
    // The error message.
    @XmlElement @JsonProperty private String message = "(null)";

    // The HTTP request parsed by the REST service. 
    @XmlElement @JsonProperty private String parsedRequest = "(null)";

    // The UTC date and time of the error.
    @XmlElement @JsonProperty private LocalDateTime utcTimestamp =
	LocalDateTime.now(Clock.systemUTC()); 

    /**
     * Protected default constructor to allow JAXB marshalling.
     */
    protected RestResponseError() {
    }

    /**
     * Constructor.
     *
     * @param message
     *          A String with the detail message.
     * @param parsedRequest
     *          A String with a copy of the parsed HTTP request contents.
     */
    public RestResponseError(String message, String parsedRequest) {
      if (message != null) {
	this.message = message;
      }

      if (parsedRequest != null) {
	this.parsedRequest = parsedRequest;
      }
    }

    /**
     * Constructor.
     *
     * @param message
     *          A String with the detail message.
     * @param parsedRequest
     *          A String with a copy of the parsed HTTP request contents.
     * @param utcTimestamp
     *          A LocalDateTime with the error date and time.
     */
    public RestResponseError(String message, String parsedRequest,
	LocalDateTime utcTimestamp) {
      if (message != null) {
	this.message = message;
      }

      if (parsedRequest != null) {
	this.parsedRequest = parsedRequest;
      }

      if (utcTimestamp != null) {
	this.utcTimestamp = utcTimestamp;
      }
    }

    /**
     * Provides the error message.
     * 
     * @return a String with the error message.
     */
    public String getMessage() {
      return message;
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
     * Provides the error date and time.
     * 
     * @return a LocalDateTime with the error date and time.
     */
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

      if (!(obj instanceof RestResponseError)) {
	return false;
      }

      RestResponseError other = (RestResponseError)obj;

      return this.message.equals(other.message)
	  && this.parsedRequest.equals(other.parsedRequest)
	  && this.utcTimestamp.equals(other.utcTimestamp);
    }
  }
}
