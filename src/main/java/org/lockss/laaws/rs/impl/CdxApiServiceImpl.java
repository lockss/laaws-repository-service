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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.lockss.laaws.error.LockssRestServiceException;
import org.lockss.laaws.rs.api.CdxApiDelegate;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.CdxRecord;
import org.lockss.laaws.rs.model.CdxRecords;
import org.archive.wayback.surt.SURTTokenizer;
import org.lockss.log.L4JLogger;
import org.springframework.beans.factory.annotation.Autowired;
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
   *          (urlquery/prefixquery), offset, limit, request.anchordate,
   *          startdate and enddate.
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
    String parsedRequest = getParsedRequest(request, collectionid);
    log.trace("Parsed request: " + parsedRequest);

    // Validate the repository.
    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    // Check that the collection exists.
    try {
      ServiceImplUtil.validateCollectionId(repo, collectionid, parsedRequest);
    } catch (IOException e) {
      String message =
	  "Cannot validate the collectionid = '" + collectionid + "'";
      log.error(message, e);
      return getErrorResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR, message,
	  e);
    }

    // Validate the pagination.
    ServiceImplUtil.validatePagination(count, startPage, parsedRequest);

    try {
      // Parse the OpenWayback query.
      Map<String, String> openWayBackQuery = parseOpenWayBackQueryString(q);
      log.trace("openWayBackQuery = {}", openWayBackQuery);

      String url = openWayBackQuery.get("url");
      log.trace("url = {}", url);

      // Add a canonical version of the query URL.
      openWayBackQuery.put("canonicalUrl", url);

      // Initialize the results.
      CdxRecords records = new CdxRecords(openWayBackQuery, charsetName);

      // Determine whether it is a prefix query.
      boolean isPrefix = openWayBackQuery.containsKey("type")
	  && openWayBackQuery.get("type").toLowerCase().equals("prefixquery");
      log.trace("isPrefix = {}", isPrefix);

      // Get the target timestamp, if any.
      String date = null;

      if (openWayBackQuery.containsKey("date")
	  && !openWayBackQuery.get("date").trim().isEmpty()) {
	date = openWayBackQuery.get("date").trim();
	log.trace("date = {}", date);
      }

      // TODO: Handle offset, limit, request.anchordate, startdate and enddate
      // in the OpenWayback query.

      // Get the results.
      getCdxRecords(collectionid, url, repo, isPrefix, count, startPage, date,
	  records);

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
   *          A String with the timestamp for the sort=closest mode.
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
  public ResponseEntity<String> getCdxPywb(String collectionid, String url,
      Integer limit, String matchType, String sort, String closest,
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
    String parsedRequest = getParsedRequest(request, collectionid);
    log.trace("Parsed request: " + parsedRequest);

    // Validate the repository.
    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    // Check that the collection exists.
    try {
      ServiceImplUtil.validateCollectionId(repo, collectionid, parsedRequest);
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

      // TODO: Handle the matchType query parameter values host, domain, range.

      // Determine whether it is an exact query.
      boolean isExact =
	  matchType == null || matchType.toLowerCase().equals("exact");
      log.trace("isExact = {}", isExact);

      if (!isExact) {
	closest = null;
      }

      // Determine whether it is a prefix query.
      boolean isPrefix =
	  matchType != null && matchType.toLowerCase().equals("prefix");
      log.trace("isPrefix = {}", isPrefix);

      Integer startPage = null;

      // Check whether there is pagination involved.
      if (limit != null) {
	// Yes: Return the first page of results.
	startPage = 1;

	// Validate the pagination.
	ServiceImplUtil.validatePagination(limit, startPage, parsedRequest);
      }

      // TODO: Handle the query parameters sort and fl.

      // Get the results.
      getCdxRecords(collectionid, url, repo, isPrefix, limit, startPage,
	  closest, records);

      // Convert the results to the right format.
      String result = null;

      if (output == null || output.trim().isEmpty()
	  || output.trim().toLowerCase().equals("cdx")) {
	result = records.toIaText();
      } else if (output.trim().toLowerCase().equals("json")) {
	result = records.toJson();
      } else {
	String errorMessage = "Invalid output request parameter: " + output;
	log.error(errorMessage);
	throw new LockssRestServiceException(HttpStatus.BAD_REQUEST,
	    errorMessage, parsedRequest);
      }

      log.trace("records.toJson() = {}", records.toJson());
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
   * Provides the parsed request for diagnostic purposes.
   * 
   * @param request
   *          An HttpServletRequest with the HTTP request.
   * @param collectionid
   *          A String with the identifier of the collection.
   * @return a String with the parsed request.
   */
  private String getParsedRequest(HttpServletRequest request,
      String collectionid) {
    return String.format("collectionid: %s, requestUrl: %s", collectionid,
	ServiceImplUtil.getFullRequestUrl(request));
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
   * @param isPrefix
   *          A boolean indicating whether the passed URL is to be used as a
   *          search prefix.
   * @param count
   *          An Integer with the count of results per page to be returned, or
   *          <code>null</code> if no limit is requested.
   * @param startPage
   *          An Integer with the page number of results, 1 based.
   * @param closest
   *          A String with the target sorting timestamp of the results.
   * @param records
   *          A CdxRecords where the resulting CDX records are stored.
   * @throws IOException
   *           if there are I/O problems.
   */
  void getCdxRecords(String collectionid, String url, LockssRepository repo,
      boolean isPrefix, Integer count, Integer startPage, String closest,
      CdxRecords records) throws IOException {
    log.debug2("collectionid = {}", collectionid);
    log.debug2("url = {}", url);
    log.debug2("isPrefix = {}", isPrefix);
    log.debug2("count = {}", count);
    log.debug2("startPage = {}", startPage);
    log.debug2("closest = {}", closest);

    Iterable<Artifact> iterable = null;

    if (isPrefix) {
      // Yes: Get from the repository the artifacts for URLs with the passed
      // prefix.
      iterable =
	  repo.getArtifactsWithPrefixAllVersionsAllAus(collectionid, url);
    } else {
      // No: Get from the repository the artifacts for the passed URL.
      iterable = repo.getArtifactsAllVersionsAllAus(collectionid, url);
    }

    // Initialize the iterator on the collection of artifacts to be returned.
    Iterator<Artifact> artIterator = iterable.iterator();

    if (closest != null && !closest.trim().isEmpty()) {
      // Yes: Return all the artifacts found sorted by temporal proximity to the
      // target timestamp.
      artIterator =
	  getArtifactsSortedByTemporalGap(artIterator, closest).iterator();
    }

    // Get the CDX records for the selected artifacts.
    getArtifactsCdxRecords(artIterator, repo, count, startPage, records);
  }

  /**
   * Provides the requested CDX records for a collection of artifacts.
   *
   * @param artIterator
   *          An Iterator<Artifact> to the artifacts for which the CDX records
   *          are requested.
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
  void getArtifactsCdxRecords(Iterator<Artifact> artIterator,
      LockssRepository repo, Integer count, Integer startPage,
      CdxRecords records) throws IOException {
    log.debug2("count = {}", count);
    log.debug2("startPage = {}", startPage);

    int lastArticleSkipped = -1;
    int lastArticleIncluded = Integer.MAX_VALUE;

    // Check whether the results are bounded in quantity.
    if (count != null) {
      // Yes: Determine the boundaries of the results to be returned.
      lastArticleSkipped = count * (startPage - 1) - 1;
      log.trace("lastArticleSkipped = {}", lastArticleSkipped);

      lastArticleIncluded = lastArticleSkipped + count;
      log.trace("lastArticleIncluded = {}", lastArticleIncluded);
    }

    // Loop through all the artifacts that are potential results.
    int iteratorCounter = 0;

    while (artIterator.hasNext()) {
      // Get the next artifact.
      Artifact artifact = artIterator.next();
      log.trace("artifact = {}", artifact);

      // Check whether this artifact needs to be included in the results.
      if (iteratorCounter > lastArticleSkipped) {
	// Yes: Get the artifact identifier.
	String artifactId = artifact.getId();
	log.trace("artifactId = {}", artifactId);

	// Create the result for this artifact.
	CdxRecord record = getCdxRecord(
	    repo.getArtifactData(artifact.getCollection(), artifactId));
	log.trace("record = {}", record);

	// Add this artifact to the results.
	records.addCdxRecord(record);
	log.trace("recordsCount = {}", records.getCdxRecordCount());
      }

      iteratorCounter++;

      // Check whether all the results to be returned have been processed.
      if (iteratorCounter > lastArticleIncluded) {
	// Yes: Stop the loop.
	break;
      }
    }
  }

  /**
   * Provides a copy of a collection of artifacts that is sorted by increasing
   * temporal gap from a target timestamp.
   * 
   * @param artIterator
   *          An Iterator<Artifact> for the original collection of artifacts.
   * @param closest
   *          A String with the target timestamp.
   * @return a List<Artifact> with the sorted collection of artifacts.
   * @throws IOException
   *           if there are problems getting the artifact data from the
   *           repository.
   */
  List<Artifact> getArtifactsSortedByTemporalGap(Iterator<Artifact> artIterator,
      String closest) throws IOException {
    log.debug2("closest = {}", closest);

    // Convert the passed CDX record timestamp to the one stored in the
    // repository.
    long targetTimestamp = CdxRecord.computeCollectiondate(closest);
    log.trace("targetTimestamp = {}", targetTimestamp);

    // Get a copy of a collection of objects that are suitable for sorting by
    // their temporal gap with respect to the target timestamp.
    List<ClosestArtifact> cas = new ArrayList<>();

    while (artIterator.hasNext()) {
      cas.add(new ClosestArtifact(artIterator.next(), targetTimestamp));
    }

    log.trace("cas.size() = {}", cas.size());

    // Sort the collection of objects by their temporal gap with respect to the
    // target timestamp.
    cas.sort(Comparator.comparing(ClosestArtifact::getGap));

    // Get the list of artifacts soted by their temporal proximity to the target
    // timestamp.
    List<Artifact> sortedArtifacts = new ArrayList<>();
    Iterator<ClosestArtifact> caIterator = cas.iterator();

    while (caIterator.hasNext()) {
      sortedArtifacts.add(caIterator.next().getArtifact());
    }

    // Return all the artifacts found sorted by their temporal proximity to the
    // target timestamp.
    return sortedArtifacts;
  }

  /**
   * Provides the CDX record of an artifact.
   * 
   * @param artifactData
   *          An ArtifactData with the artifact data.
   * @return a CdxRecord with the CDX record of the artifact.
   * @throws IOException if there are I/O problems.
   */
  CdxRecord getCdxRecord(ArtifactData artifactData) throws IOException {
    log.debug2("artifactData = {}", artifactData);

    // Initialize the result for this artifact.
    CdxRecord record = new CdxRecord();

    String artifactUrl = artifactData.getIdentifier().getUri();
    log.trace("artifactUrl = {}", artifactUrl);

    // Set the sort key.
    String urlSortKey = SURTTokenizer.exactKey(artifactUrl);
    log.trace("urlSortKey = {}", urlSortKey);
    record.setUrlSortKey(urlSortKey);

    // Set the artifact timestamp.
    long timestamp =
	CdxRecord.computeNumericTimestamp(artifactData.getCollectionDate());
    log.trace("timestamp = {}", timestamp);
    record.setTimestamp(timestamp);

    // Set the artifact URL.
    record.setUrl(artifactUrl);

    // Set the artifact MIME type.
    record.setMimeType(artifactData.getMetadata().getContentType().toString());

    // Set the artifact HTTP status.
    record.setHttpStatus(artifactData.getHttpStatus().getStatusCode());

    // Set the artifact digest.
    record.setDigest(artifactData.getContentDigest());

    // Set the artifact content length.
    record.setLength(artifactData.getContentLength());

    // Set the artifact offset. Each artifact has its own archive.
    record.setOffset(0);

    ArtifactIdentifier artifactIdentifier = artifactData.getIdentifier();

    // Set the artifact archive name.
    record.setArchiveName(ServiceImplUtil.getArtifactArchiveName(
	artifactIdentifier.getCollection(), artifactIdentifier.getId()));

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

    return new ResponseEntity<String>(ServiceImplUtil.toJsonError(
	status.value(), errorMessage), status);
  }

  /**
   * An artifact with temporal proximity to a target timestamp.
   */
  static class ClosestArtifact {
    // The artifact for which the temporal proximity to a target timestamp is
    // requested.
    private final Artifact artifact;

    // The temporal gap between the artifact timestamp and the target timestamp
    // as an absolute value in milliseconds.
    private final long gap;

    /**
     * Constructor.
     *
     * @param artifact
     *          An Artifact with the artifact for which the temporal proximity
     *          to a target timestamp is requested.
     * @param targetTimestamp
     *          A long with the target timestamp expressed as milliseconds since
     *          the epoch.
     */
    ClosestArtifact(Artifact artifact, long targetTimestamp) {
      this.artifact = artifact;

      // Calculate the temporal gap.
      gap = Math.abs(artifact.getCollectionDate() - targetTimestamp);
    }

    /**
     * Provides the artifact.
     * 
     * @return an Artifact with the artifact in this object.
     */
    Artifact getArtifact() {
      return artifact;
    }

    /**
     * Provides the temporal gap between the artifact timestamp and the target
     * timestamp as an absolute value in milliseconds.
     * 
     * @return a long with the temporal gap.
     */
    long getGap() {
      return gap;
    }
  }
}
