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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Enumeration;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.DeferredFileOutputStream;
import org.lockss.laaws.rs.api.WarcsApiDelegate;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.log.L4JLogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

/**
 * Service for streaming WARC archives.
 */
@Service
public class WarcsApiServiceImpl implements WarcsApiDelegate {
  private static L4JLogger log = L4JLogger.getLogger();

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
  public WarcsApiServiceImpl(HttpServletRequest request) {
    this.request = request;
  }

  /**
   * Provides the contents of a WARC archive.
   * 
   * @param fileName
   *          A String with the name of the requested WARC archive.
   * @param accept
   *          A String with the Accept request header.
   * @param acceptEncoding
   *          A String with the Accept-Encoding request header.
   * @param range
   *          A String with the Range request header.
   * @return a {@code ResponseEntity<Resource>} with the contents of the
   *         requested WARC archive.
   */
  @Override
  public ResponseEntity<Resource> getWarcArchive(String fileName, String accept,
      String acceptEncoding, String range) {
    log.debug2("fileName = {}", fileName);
    log.debug2("accept = {}", accept);
    log.debug2("acceptEncoding = {}", acceptEncoding);
    log.debug2("range = {}", range);

    Enumeration<String> headerNames = request.getHeaderNames();
    while (headerNames.hasMoreElements()) {
      String name = headerNames.nextElement();
      log.trace("header name = {}, value = {}", name, request.getHeader(name));
    }

    log.trace("parameterMap = {}", request.getParameterMap());

    // The parsed request for diagnostic purposes.
    String parsedRequest = String.format("fileName: %s, requestUrl: %s",
	fileName, ServiceImplUtil.getFullRequestUrl(request));
    log.trace("Parsed request: " + parsedRequest);

    // Validate the repository.
    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      String collectionId = ServiceImplUtil.getArchiveFilenameCollectionId(
	  fileName, parsedRequest);
      log.trace("collectionId = {}", collectionId);

      // Get the artifact identifier.
      String artifactId = ServiceImplUtil.getArchiveFilenameArtifactId(
	  fileName, parsedRequest);
      log.trace("artifactId = {}", artifactId);

      // Get the data for the artifact in this collection, if it exists.
      ArtifactData artifactData =
	  repo.getArtifactData(collectionId, artifactId);
      log.trace("artifactData = {}", artifactData);

      // Handle a missing artifact.
      if (artifactData == null) {
	throw new IllegalArgumentException("No artifact '" + artifactId
	    + "' in repository");
      }

      InputStream inputStream = null;
      long warcRecordLength = 0L;

      // The temporary file where to store the WARC record, if necessary.
      File warcRecordFile = File.createTempFile("getWarcArchive", ".warc");
      warcRecordFile.deleteOnExit();

      // Get an input stream to the WARC record to be returned.
      try (DeferredFileOutputStream dfos =
	  new DeferredFileOutputStream((int)FileUtils.ONE_MB, warcRecordFile)) {
	// Get the WARC record.
	WarcArtifactDataStore.writeArtifactData(artifactData, dfos);
	dfos.close();

	// Check whether the temporary file was not needed.
	if (dfos.isInMemory()) {
	  // Yes: The InputStream comes from the byte array in memory.
	  log.trace("WARC record is in memory");
	  byte[] warcRecordBytes = dfos.getData();
	  warcRecordLength = warcRecordBytes.length;
	  inputStream = new ByteArrayInputStream(warcRecordBytes);
	} else {
	  // No: The InputStream comes from the disk file.
	  log.trace("WARC record is in in file '{}'", warcRecordFile);
	  warcRecordLength = warcRecordFile.length();
	  inputStream = new FileInputStream(warcRecordFile);
	}
      }

      // Get the response headers.
      HttpHeaders headers = new HttpHeaders();
      headers.set("Content-Type", "application/warc");
      //headers.set("Content-Encoding", "gzip");
      headers.setContentLength(warcRecordLength);
      log.trace("headers = {}", headers);

      return new ResponseEntity<Resource>(new InputStreamResource(inputStream),
	  headers, HttpStatus.OK);
    } catch (IllegalArgumentException iae) {
      String message =
	  "Cannot get the archive for fileName = '" + fileName + "'";
      log.error(message, iae);
      return getErrorResponseEntity(HttpStatus.BAD_REQUEST, message, iae);
    } catch (Exception e) {
      String message =
	  "Cannot get the archive for fileName = '" + fileName + "'";
      log.error(message, e);
      return getErrorResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR, message,
	  e);
    }
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
  private ResponseEntity<Resource> getErrorResponseEntity(HttpStatus status,
      String message, Exception e) {
    String errorMessage = message;

    if (e != null) {
      if (errorMessage == null) {
	errorMessage = e.getMessage();
      } else {
	errorMessage = errorMessage + " - " + e.getMessage();
      }
    }

    String result = ServiceImplUtil.toJsonError(status.value(), errorMessage);
    InputStream is =
	new ByteArrayInputStream(result.getBytes(Charset.forName("UTF-8")));
    return new ResponseEntity<Resource>(new InputStreamResource(is), status);
  }
}
