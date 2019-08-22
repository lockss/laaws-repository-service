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
import java.util.stream.StreamSupport;
import javax.servlet.http.HttpServletRequest;
import org.json.JSONObject;
import org.lockss.laaws.error.LockssRestServiceException;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.log.L4JLogger;
import org.springframework.http.HttpStatus;

/**
 * Utility method used in the service controllers.
 */
public class ServiceImplUtil {
  private static L4JLogger log = L4JLogger.getLogger();

  private static String archiveFileExtension = ".warc";
  private static String archiveFileSeparator = ":";

  /**
   * Provides the full URL of the request.
   * 
   * @param request
   *          An HttpServletRequest with the HTTP request.
   * 
   * @return a String with the full URL of the request.
   */
  static String getFullRequestUrl(HttpServletRequest request) {
    return "'" + request.getMethod() + " " + request.getRequestURL() + "?"
	+ request.getQueryString() + "'";
  }

  /**
   * Verifies that the repository is ready.
   * 
   * @param repo
   *          A LockssRepository with the repository.
   * @param parsedRequest
   *          A String with the parsed request for diagnostic purposes.
   */
  static void checkRepositoryReady(LockssRepository repo,
      String parsedRequest) {
    if (!repo.isReady()) {
      String errorMessage = "LOCKSS repository is not ready";
      throw new LockssRestServiceException(HttpStatus.SERVICE_UNAVAILABLE,
	  errorMessage, parsedRequest);
    }
  }

  /**
   * Verifies that the collection exists.
   * 
   * @param repo
   *          A LockssRepository with the repository.
   * @param collectionid
   *          A String with the identifier of the collection.
   * @param parsedRequest
   *          A String with the parsed request for diagnostic purposes.
   * @exception IOException
   *              if there are problems accessing the repository.
   */
  static void validateCollectionId(LockssRepository repo, String collectionid,
      String parsedRequest) throws IOException {
    if (!StreamSupport.stream(repo.getCollectionIds().spliterator(), false)
	.anyMatch(name -> collectionid.equals(name))) {
      String errorMessage = "The collection does not exist";
      log.warn(errorMessage);
      log.warn("Parsed request: {}", parsedRequest);

      throw new LockssRestServiceException(HttpStatus.NOT_FOUND, errorMessage, 
	  parsedRequest);
    }

    log.debug2("collectionid '{}' is valid.", collectionid);
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
  static void validatePagination(Integer count, Integer startPage,
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
   * Provides the archive name to be used for an artifact.
   * 
   * If no extension is provided, one is added automatically  at the other end.
   * 
   * @param collectionid
   *          A String with the identifier of the collection.
   * @param artifactId
   *          A String with the identifier of the artifact.
   * @return a String with the artifact archive name.
   */
  static String getArtifactArchiveName(String collectionid, String artifactId) {
   return collectionid + archiveFileSeparator + artifactId
       + archiveFileExtension; 
  }

  /**
   * Provides the collection identifier embedded in the archive file name.
   * 
   * @param fileName
   *          A String with the file name.
   * @param parsedRequest
   *          A String with the parsed request for diagnostic purposes.
   * @return a String with the collection identifier.
   */
  static String getArchiveFilenameCollectionId(String fileName,
      String parsedRequest) {
    int separatorLocation =
	getArchiveFilenameSeparator(fileName, parsedRequest);

    return fileName.substring(0, separatorLocation);
  }

  /**
   * Provides the artifact identifier embedded in the archive file name.
   * 
   * @param fileName
   *          A String with the file name.
   * @param parsedRequest
   *          A String with the parsed request for diagnostic purposes.
   * @return a String with the artifact identifier.
   */
  static String getArchiveFilenameArtifactId(String fileName,
      String parsedRequest) {
    int separatorLocation =
	getArchiveFilenameSeparator(fileName, parsedRequest);

    return fileName.substring(separatorLocation + archiveFileSeparator.length(),
	fileName.length() - archiveFileExtension.length());
  }

  /**
   * Provides the location of the separator between the collection and artifact
   * identifiers embedded in the archive file name.
   * 
   * @param fileName
   *          A String with the file name.
   * @param parsedRequest
   *          A String with the parsed request for diagnostic purposes.
   * @return an int with the location of the separator.
   */
  private static int getArchiveFilenameSeparator(String fileName,
      String parsedRequest) {
    int separatorLocation = fileName.lastIndexOf(archiveFileSeparator);
    log.trace("separatorLocation = {}", separatorLocation);

    if (separatorLocation < 1) {
      String errorMessage = "Missing separator '" + archiveFileSeparator
	  + "' in filename: " + fileName;
      log.error(errorMessage);
      throw new LockssRestServiceException(HttpStatus.BAD_REQUEST,
	  errorMessage, parsedRequest);
    }

    return separatorLocation;
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
  static String toJsonError(int code, String message) {
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
