/*

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import javax.servlet.http.HttpServletRequest;
import org.json.JSONObject;
import org.lockss.laaws.error.LockssRestServiceException;
import org.lockss.laaws.rs.api.CdxApiDelegate;
import org.lockss.laaws.rs.core.LockssRepository;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.CdxRecord;
import org.lockss.laaws.rs.model.CdxRecords;
import org.archive.wayback.surt.SURTTokenizer;
import org.lockss.log.L4JLogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

/**
 * Service for providing CDX records.
 */
@Service
public class CdxApiServiceImpl implements CdxApiDelegate {
  private static L4JLogger log = L4JLogger.getLogger();

  // Name of the charset used.
  private static String charsetName = StandardCharsets.UTF_8.name();

  // The repository.
  @Autowired
  LockssRepository repo;

  // The HTTP request.
  private final HttpServletRequest request;

  /**
   * Constructor for autowiring.
   * 
   * @param request
   *          An HttpServletRequest with the HTTP request.
   */
  @Autowired
  public CdxApiServiceImpl(HttpServletRequest request) {
    this.request = request;
  }

  /**
   * Provides the OpenWayback CDX records of a URL in a collection.
   * 
   * @param collectionid
   *          A String with the identifier of the collection.
   * @param q
   *          A String with the query string. Supported fields are url, type
   *          (urlquery/prefixquery), offset and limit.
   * @param count
   *          An Integer with the count of results per page to be returned.
   * @param startPage
   *          An Integer with the page number of results, 1 based.
   * @param accept
   *          A String with the Accept request header.
   * @param acceptEncoding
   *          A String with the Accept-Encoding request header.
   * @return a {@code ResponseEntity<String>} with the requested OpenWayback CDX
   *         records.
   */
  @Override
  public ResponseEntity<String> getCdxOwb(String collectionid, String q,
      Integer count, Integer startPage, String accept, String acceptEncoding) {
    log.debug2("collectionid = {}", collectionid);
    log.debug2("q = {}", q);
    log.debug2("count = {}", count);
    log.debug2("startPage = {}", startPage);
    log.debug2("accept = {}", accept);
    log.debug2("acceptEncoding = {}", acceptEncoding);

    Enumeration<String> headerNames = request.getHeaderNames();
    while (headerNames.hasMoreElements()) {
      String name = headerNames.nextElement();
      log.trace("header name = {}, value = {}", name, request.getHeader(name));
    }

    log.trace("parameterMap = {}", request.getParameterMap());

    // The parsed request for diagnostic purposes.
    String parsedRequest = String.format("collectionid: %s, requestUrl: %s",
	collectionid, getFullRequestUrl());
    log.trace("Parsed request: " + parsedRequest);

    // Validate the repository.
    checkRepositoryReady(parsedRequest);

    // Check that the collection exists.
    try {
      validateCollectionId(collectionid, parsedRequest);
    } catch (IOException e) {
      String message =
	  "Cannot validate the collectionid = '" + collectionid + "'";
      log.error(message, e);
      return getErrorResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR, message,
	  e);
    }

    // Validate the pagination.
    validatePagination(count, startPage, parsedRequest);

    try {
      // Parse the OpenWayback query.
      Map<String, String> openWayBackQuery = parseOpenWayBackQueryString(q);
      log.trace("openWayBackQuery = {}", openWayBackQuery);

      String url = openWayBackQuery.get("url");
      log.trace("url = {}", url);

      // Add a canonical version of the query URL.
      openWayBackQuery.put("canonicalUrl",
	  new AggressiveUrlCanonicalizer().canonicalize(url));

      // Initialize the results.
      CdxRecords records = new CdxRecords(openWayBackQuery, charsetName);

      // Get the results.
      getCdxRecords(collectionid, url, repo, count, startPage, records);

      // Convert the results to XML.
      String result = records.toXmlText();
      log.debug2("result = {}", result);

      return new ResponseEntity<String>(result, HttpStatus.OK);
    } catch (IllegalArgumentException | UnsupportedEncodingException bre) {
      String message = "Cannot get the CDX records for collectionid = '"
	  + collectionid + "', q = '" + q + "'";
      log.error(message, bre);
      return getErrorResponseEntity(HttpStatus.BAD_REQUEST, message, bre);
    } catch (Exception e) {
      String message = "Cannot get the CDX records for collectionid = '"
	  + collectionid + "', q = '" + q + "'";
      log.error(message, e);
      return getErrorResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR, message,
	  e);
    }
  }

  /**
   * Provides the PyWayback CDX records of a URL in a collection.
   * 
   * @param collectionid
   *          A String with the identifier of the collection.
   * @param url
   *          A String with the URL for which the CDX records are requested.
   * @param limit
   *          An Integer with the limit.
   * @param matchType
   *          A String with the type of match requested.
   * @param sort
   *          A String with the type of sort requested.
   * @param closest
   *          An Integer with the timestamp for the sort=closest mode.
   * @param output
   *          A String with the output format requested.
   * @param fl
   *          A String with the comma-separated list of fields to include in the
   *          result.
   * @param accept
   *          A String with the Accept request header.
   * @param acceptEncoding
   *          A String with the Accept-Encoding request header.
   * @return a {@code ResponseEntity<String>} with the requested PyWayback CDX
   *         records.
   */
  @Override
  public ResponseEntity<String> getCdxPwb(String collectionid, String url,
      Integer limit, String matchType, String sort, Integer closest,
      String output, String fl, String accept, String acceptEncoding) {
    log.debug2("collectionid = {}", collectionid);
    log.debug2("url = {}", url);
    log.debug2("limit = {}", limit);
    log.debug2("matchType = {}", matchType);
    log.debug2("sort = {}", sort);
    log.debug2("closest = {}", closest);
    log.debug2("output = {}", output);
    log.debug2("fl = {}", fl);
    log.debug2("accept = {}", accept);
    log.debug2("acceptEncoding = {}", acceptEncoding);

    Enumeration<String> headerNames = request.getHeaderNames();
    while (headerNames.hasMoreElements()) {
      String name = headerNames.nextElement();
      log.trace("header name = {}, value = {}", name, request.getHeader(name));
    }

    log.trace("parameterMap = {}", request.getParameterMap());

    // The parsed request for diagnostic purposes.
    String parsedRequest = String.format("collectionid: %s, requestUrl: %s",
	collectionid, getFullRequestUrl());
    log.trace("Parsed request: " + parsedRequest);

    checkRepositoryReady(parsedRequest);

    // Check that the collection exists.
    try {
      validateCollectionId(collectionid, parsedRequest);
    } catch (IOException e) {
      String message =
	  "Cannot validate the collectionid = '" + collectionid + "'";
      log.error(message, e);
      return getErrorResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR, message,
	  e);
    }

    try {
      // Initialize the results.
      CdxRecords records = new CdxRecords();

      // Get the results.
      getCdxRecords(collectionid, url, repo, null, null, records);

      // Convert the results to XML.
      String result = records.toIaText();
      log.debug2("result = {}", result);

      return new ResponseEntity<String>(result, HttpStatus.OK);
    } catch (IllegalArgumentException iae) {
      String message = "Cannot get the CDX records for collectionid = '"
	  + collectionid + "', url = '" + url + "'";
      log.error(message, iae);
      return getErrorResponseEntity(HttpStatus.BAD_REQUEST, message, iae);
    } catch (Exception e) {
      String message = "Cannot get the CDX records for collectionid = '"
	  + collectionid + "', url = '" + url + "'";
      log.error(message, e);
      return getErrorResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR, message,
	  e);
    }
  }

  /**
   * Provides the full URL of the request.
   * 
   * @return a String with the full URL of the request.
   */
  private String getFullRequestUrl() {
    return "'" + request.getMethod() + " " + request.getRequestURL() + "?"
	+ request.getQueryString() + "'";
  }

  /**
   * Verifies that the repository is ready.
   * 
   * @param parsedRequest
   *          A String with the parsed request for diagnostic purposes.
   */
  private void checkRepositoryReady(String parsedRequest) {
    if (!repo.isReady()) {
      String errorMessage = "LOCKSS repository is not ready";
      throw new LockssRestServiceException(HttpStatus.SERVICE_UNAVAILABLE,
	  errorMessage, parsedRequest);
    }
  }

  /**
   * Verifies that the collection exists.
   * 
   * @param parsedRequest
   *          A String with the parsed request for diagnostic purposes.
   */

  private void validateCollectionId(String collectionid, String parsedRequest)
      throws IOException {
    if (!StreamSupport.stream(repo.getCollectionIds().spliterator(), false)
	.anyMatch(name -> collectionid.equals(name))) {
      String errorMessage = "The collection does not exist";
      log.warn(errorMessage);
      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(HttpStatus.NOT_FOUND, errorMessage, 
	  parsedRequest);
    }

    log.debug("collectionid '" + collectionid + "' is valid.");
  }

  /**
   * Validates the pagination request parameters.
   * 
   * @param count
   *          An Integer with the count of results per page to be returned.
   * @param startPage
   *          An Integer with the page number of results, 1 based.
   * @param parsedRequest
   *          A String with the parsed request for diagnostic purposes.
   */
  private void validatePagination(Integer count, Integer startPage,
      String parsedRequest) {
    log.debug2("count = {}", count);
    log.debug2("startPage = {}", startPage);
    log.debug2("parsedRequest = {}", parsedRequest);

    if (count == null && startPage == null) {
      log.debug2("Pagination request parameters are valid");
      return;
    }

    if (count != null && startPage != null && count > 0 && startPage > 0) {
      log.debug2("Pagination request parameters are valid");
      return;
    }

    String errorMessage = "Invalid pagination request: count = " + count
	  + ", startPage = " + startPage;
    log.error(errorMessage);
    throw new LockssRestServiceException(HttpStatus.BAD_REQUEST, errorMessage, 
	  parsedRequest);
  }

  /**
   * Parses an OpenWayBack query into its components.
   * 
   * @param q
   *          A String with the OpenWayBack query.
   * @return a Map<String, String> with the parsed elements of the OpenWayBack
   *         query.
   * @exception UnsupportedEncodingException
   *              if there are problems parsing the query.
   */
  private static Map<String, String> parseOpenWayBackQueryString(String q)
      throws UnsupportedEncodingException {
    log.debug2("q = {}", q);

    try {
      Map<String, String> queryMap = new HashMap<>();

      // Loop through the query elements.
      for (String queryElement : q.split(" ")) {
	log.trace("queryElement = {}", queryElement);

	// Parse this element.
	String[] elementFields = queryElement.split(":", 2);
	log.trace("elementFields[0] = {}", elementFields[0]);

	// Get the value of this element.
	String value = URLDecoder.decode(elementFields[1], charsetName);
	log.trace("value = {}", value);

	// Add this element to the map.
	queryMap.put(elementFields[0], value);
      }

      log.debug2("queryMap = {}", queryMap);
      return queryMap;
    } catch (UnsupportedEncodingException uee) {
      log.error("Exception caught parsing query", uee);
      throw uee;
    }
  }

  /**
   * Provides the requested CDX records.
   *
   * @param collectionid
   *          A String with the identifier of the collection.
   * @param url
   *          A String with the URL of the artifacts for which the CDX records
   *          are requested.
   * @param repo
   *          A LockssRepository with the repository.
   * @param count
   *          An Integer with the count of results per page to be returned, or
   *          <code>null</code> if no limit is requested.
   * @param startPage
   *          An Integer with the page number of results, 1 based.
   * @param records
   *          A CdxRecords where the resulting CDX records are stored.
   * @throws IOException
   *           if there are I/O problems.
   */
  private void getCdxRecords(String collectionid, String url,
      LockssRepository repo, Integer count, Integer startPage,
      CdxRecords records) throws IOException {
    log.debug2("collectionid = {}", collectionid);
    log.debug2("url = {}", url);
    log.debug2("count = {}", count);
    log.debug2("startPage = {}", startPage);

    // Get from the repository the artifacts for the passed URL.
    Iterable<Artifact> iterable =
	repo.getArtifactAllVersions(collectionid, null, url);

    // Initialize the collection of artifacts for the results to be returned.
    List<Artifact> artifacts = new ArrayList<>();

    // Check whether there is no limit to the results to be returned.
    if (count == null) {
      // Yes: Return all the artifacts found.
      iterable.forEach(artifacts::add);
    } else {
      // No: Determine the boundaries of the results to be returned.
      int lastSkipped = count * (startPage -1) - 1;
      log.trace("lastSkipped = {}", lastSkipped);

      int lastIncluded = lastSkipped + count;
      log.trace("lastIncluded = {}", lastIncluded);

      // Loop through all the artifacts that are potential results.
      int iteratorCounter = 0;

      Iterator<Artifact> iterator = iterable.iterator();

      while (iterator.hasNext()) {
	// Get the next artifact.
	Artifact artifact = iterator.next();

	// Check whether this artifact needs to be included in the results.
	if (iteratorCounter > lastSkipped) {
	  // Yes: Include it in the results.
	  artifacts.add(artifact);
	}

	iteratorCounter++;

	// Check whether all the results to be returned have been processed.
	if (iteratorCounter > lastIncluded) {
	  // Yes: Stop the loop.
	  break;
	}
      }
    }

    log.trace("artifacts.size() = {}", artifacts.size());

    // Loop through all the artifacts involved in the results.
    for (Artifact artifact : artifacts) {
      // Get the artifact identifier.
      String artifactId = artifact.getId();
      log.trace("artifactId = {}", artifactId);

      // Create the result for this artifact.
      CdxRecord record = getCdxRecord(collectionid, url, artifactId);
      log.trace("record = {}", record);

      // Add this artifact to the results.
      records.addCdxRecord(record);
      log.trace("recordsCount = {}", records.getCdxRecordCount());
    }
  }

  /**
   * Provides the CDX record of an artifact.
   * 
   * @param collectionid
   *          A String with the identifier of the collection.
   * @param url
   *          A String with the URL of the artifact for which the CDX record is
   *          requested.
   * @param artifactId
   *          A String with the identifier of the artifact.
   * @return a CdxRecord with the CDX record of the artifact.
   * @throws IOException if there are I/O problems.
   */
  private CdxRecord getCdxRecord(String collectionid, String url,
      String artifactId) throws IOException {
    log.debug2("collectionid = {}", collectionid);
    log.debug2("url = {}", url);
    log.debug2("artifactId = {}", artifactId);

    // Get the artifact data.
    ArtifactData artifactData = repo.getArtifactData(collectionid, artifactId);
    log.trace("artifactData = {}", artifactData);

    // Initialize the result for this artifact.
    CdxRecord record = new CdxRecord();

    // Set the sort key.
    String urlSortKey = SURTTokenizer.exactKey(url);
    log.trace("urlSortKey = {}", urlSortKey);
    record.setUrlSortKey(urlSortKey);

    // Get the artifact metadata.
    HttpHeaders headers = artifactData.getMetadata();
    log.trace("headers = {}", headers);

    // Set the artifact timestamp.
    String timestamp = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
	.format(LocalDateTime.ofEpochSecond(headers.getDate()/1000, 0,
	    ZoneOffset.UTC));
    log.trace("timestamp = {}", timestamp);
    record.setTimestamp(Long.parseLong(timestamp));

    // Set the artifact URL.
    record.setUrl(url);

    // Set the artifact MIME type.
    record.setMimeType(headers.getContentType().toString());

    // Set the artifact HTTP status.
    record.setHttpStatus(artifactData.getHttpStatus().getStatusCode());

    // Set the artifact digest.
    record.setDigest(artifactData.getContentDigest());

    // Set the artifact content length.
    record.setLength(artifactData.getContentLength());

    // Set the artifact offset. Each artifact has its own archive.
    record.setOffset(0);

    // Set the artifact archive name. If no extension is provided, one is added
    // at the other end.
    record.setArchiveName(artifactId + ".warc");

    log.debug2("record = {}", record);
    return record;
  }

  /**
   * Provides the response entity when there is an error.
   * 
   * @param status
   *          An HttpStatus with the error HTTP status.
   * @param message
   *          A String with the error message.
   * @param e
   *          An Exception with the error exception.
   * @return a {@code ResponseEntity<List<String>>} with the error response
   *         entity.
   */
  private ResponseEntity<String> getErrorResponseEntity(HttpStatus status,
      String message, Exception e) {
    String errorMessage = message;

    if (e != null) {
      if (errorMessage == null) {
	errorMessage = e.getMessage();
      } else {
	errorMessage = errorMessage + " - " + e.getMessage();
      }
    }

    return new ResponseEntity<String>(toJsonError(status.value(), errorMessage),
	status);
  }

  /**
   * Provides an error message formatted in JSON.
   * 
   * @param code
   *          An int with the code of the error to be formatted.
   * @param message
   *          A String with the message of the error to be formatted.
   * @return a String with the error message formatted in JSON.
   */
  private static String toJsonError(int code, String message) {
    JSONObject errorElement = new JSONObject();
    errorElement.put("code", code);

    if (message == null) {
      message = "";
    }

    errorElement.put("message", message);

    JSONObject responseBody = new JSONObject();
    responseBody.put("error", errorElement);
    return responseBody.toString();
  }
}
