/*
 * Copyright (c) 2017-2018, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.laaws.rs.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.ApiParam;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.lockss.laaws.error.LockssRestServiceException;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.util.ArtifactConstants;
import org.lockss.laaws.rs.util.ArtifactDataFactory;
import org.lockss.laaws.rs.util.ArtifactDataUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@RestController
public class CollectionsApiController implements CollectionsApi {
  private final static Log log = LogFactory.getLog(CollectionsApiController.class);
  public static final String APPLICATION_HTTP_RESPONSE_VALUE =
      "application/http;msgtype=response";
  public static final MediaType APPLICATION_HTTP_RESPONSE =
      MediaType.parseMediaType(APPLICATION_HTTP_RESPONSE_VALUE);

  @Autowired
  LockssRepository repo;

  private final ObjectMapper objectMapper;

  private final HttpServletRequest request;

  @org.springframework.beans.factory.annotation.Autowired
  public CollectionsApiController(ObjectMapper objectMapper,
      HttpServletRequest request) {
    this.objectMapper = objectMapper;
    this.request = request;
  }

  /**
   * GET /collections:
   * Returns a list of collection names managed by this repository.
   *
   * @return a List<String> with the collection names.
   */
  @Override
  public ResponseEntity<List<String>> collectionsGet() {
    log.debug("collectionsGet(): Invoked");
    List<String> collectionIds = new ArrayList<>();
    try {
      Iterable<String> ids = repo.getCollectionIds();
      log.debug("collectionsGet(): ids.iterator().hasNext() = "
	  + ids.iterator().hasNext());
      ids.forEach(x -> collectionIds.add(x));
    } catch (IOException e) {
      String errorMessage =
	  "IOException was caught trying to enumerate collection IDs";

      log.warn(errorMessage);

      throw new LockssRestServiceException(errorMessage, e,
	  HttpStatus.INTERNAL_SERVER_ERROR);
    }

    log.debug("collectionsGet(): collectionIds = " + collectionIds);
    return new ResponseEntity<>(collectionIds, HttpStatus.OK);
  }

  /**
   * DELETE /collections/{collectionid}/artifacts/{artifactid}:
   * Deletes an artifact from a collection managed by this repository.
   *
   * @param collectionid
   *          A String with the name of the collection containing the artifact.
   * @param artifactid
   *          A String with the Identifier of the artifact.
   * @return a {@code ResponseEntity<Void>}.
   */
  @Override
  public ResponseEntity<Void> collectionsCollectionidArtifactsArtifactidDelete(
      @ApiParam(value = "Collection containing the artifact",required=true)
      @PathVariable("collectionid") String collectionid,
      @ApiParam(value = "Identifier of the artifact",required=true)
      @PathVariable("artifactid") String artifactid
      ) {
    try {
      // Check that the artifact exists
      if (!repo.artifactExists(artifactid)) {
	String errorMessage = "The artifact to be deleted does not exist";
	log.warn(errorMessage);

	String parsedRequest = String.format("collectionid: %s, artifactid: %s",
	    collectionid, artifactid);

	log.warn("Parsed request: " + parsedRequest);

	throw new LockssRestServiceException(errorMessage, HttpStatus.NOT_FOUND,
	    parsedRequest);
      }

      // Remove the artifact from the artifact store and index
      repo.deleteArtifact(collectionid, artifactid);
      return new ResponseEntity<>(HttpStatus.OK);

    } catch (IOException e) {
      String errorMessage = String.format(
	  "IOException occurred while attempting to delete artifact from repository (artifactId: %s)",
	  artifactid);

      log.warn(errorMessage, e);

      String parsedRequest = String.format("collectionid: %s, artifactid: %s",
	  collectionid, artifactid);
   
      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(errorMessage, e,
	  HttpStatus.INTERNAL_SERVER_ERROR, parsedRequest);
    }
  }

  /**
   * GET /collections/{collectionid}/artifacts/{artifactid}:
   * Retrieves an artifact from the repository.
   *
   * @param collectionid
   *          A String with the name of the collection containing the artifact.
   * @param artifactid
   *          A String with the Identifier of the artifact.
   * @return a {@code ResponseEntity<StreamingResponseBody>}.
   */
  @Override
  public ResponseEntity<StreamingResponseBody> collectionsCollectionidArtifactsArtifactidGet(
      @ApiParam(value = "Collection containing the artifact",required=true)
      @PathVariable("collectionid") String collectionid,
      @ApiParam(value = "Identifier of the artifact",required=true)
      @PathVariable("artifactid") String artifactid,
      @ApiParam(value = "Content type to return" , allowableValues="application/http, application/warc, multipart/related", defaultValue="multipart/related")
      @RequestHeader(value="Accept", required=false) String accept
      ) {
    log.info(String.format("Retrieving artifact: %s from collection %s",
	artifactid, collectionid));

    try {
      // Make sure the artifact exists
      if (!repo.artifactExists(artifactid)) {
	String errorMessage = "The artifact to be retrieved does not exist";
	log.warn(errorMessage);

	String parsedRequest = String.format(
	    "collectionid: %s, artifactid: %s, accept: %s",
	    collectionid, artifactid, accept);

	log.warn("Parsed request: " + parsedRequest);

	throw new LockssRestServiceException(errorMessage, HttpStatus.NOT_FOUND,
	    parsedRequest);
      }

      // Retrieve the ArtifactData from the artifact store
      ArtifactData artifactData =
	  repo.getArtifactData(collectionid, artifactid);

      // Setup HTTP response headers
      HttpHeaders headers = new HttpHeaders();
      headers.setContentType(
	  MediaType.parseMediaType("application/http; msgtype=response"));
      headers.setContentLength(artifactData.getContentLength());

      // Include LOCKSS repository headers in the HTTP response
      ArtifactIdentifier id = artifactData.getIdentifier();
      headers.set(ArtifactConstants.ARTIFACTID_ID_KEY, id.getId());
      headers.set(ArtifactConstants.ARTIFACTID_COLLECTION_KEY,
	  id.getCollection());
      headers.set(ArtifactConstants.ARTIFACTID_AUID_KEY, id.getAuid());
      headers.set(ArtifactConstants.ARTIFACTID_URI_KEY, id.getUri());
      headers.set(ArtifactConstants.ARTIFACTID_VERSION_KEY,
	  String.valueOf(id.getVersion()));

      return new ResponseEntity<>(
	  outputStream -> {
	    try {
	      ArtifactDataUtil.writeHttpResponse(
		  ArtifactDataUtil.getHttpResponseFromArtifact(
		      artifactData.getIdentifier(),
		      artifactData.getHttpStatus(),
		      artifactData.getMetadata(),
		      artifactData.getInputStream()
		      ),
		  outputStream
		  );
	    } catch (HttpException e) {
	      e.printStackTrace();
	    }
	  },
	  headers,
	  HttpStatus.OK
	  );
    } catch (IOException e) {
      String errorMessage = String.format(
	  "IOException occurred while attempting to retrieve artifact from repository (artifactId: %s)",
	  artifactid);

      log.warn(errorMessage, e);

      String parsedRequest = String.format(
	  "collectionid: %s, artifactid: %s, accept: %s",
	  collectionid, artifactid, accept);
   
      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(errorMessage, e,
	  HttpStatus.INTERNAL_SERVER_ERROR, parsedRequest);
    }
  }

  /**
   * PUT /collections/{collectionid}/artifacts/{artifactid}:
   * Updates an artifact's properties
   *
   * Currently limited to updating an artifact's committed status.
   *
   * @param collectionid
   *          A String with the name of the collection containing the artifact.
   * @param artifactid
   *          A String with the Identifier of the artifact.
   * @param committed
   *          A Boolean with the artifact committed status.
   * @return a {@code ResponseEntity<Artifact>}.
   */
  @Override
  public ResponseEntity<Artifact> collectionsCollectionidArtifactsArtifactidPut(
      @ApiParam(value = "Collection containing the artifact",required=true)
      @PathVariable("collectionid") String collectionid,
      @ApiParam(value = "Identifier of the artifact",required=true)
      @PathVariable("artifactid") String artifactid,
      @ApiParam(value = "New commit status of artifact")
      @RequestParam(value="committed", required=false)  Boolean committed
      ) {
    // Return bad request if new commit status has not been passed
    if (committed == null) {
      String errorMessage = "The committed status cannot be null";
      log.warn(errorMessage);

      String parsedRequest = String.format(
	  "collectionid: %s, artifactid: %s, committed: %s",
	  collectionid, artifactid, committed);

      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(errorMessage, HttpStatus.BAD_REQUEST,
	  parsedRequest);
    }

    try {
      // Make sure that the artifact exists
      if (!repo.artifactExists(artifactid)) {
	String errorMessage = "The artifact to be updated does not exist";
	log.warn(errorMessage);

	String parsedRequest = String.format(
	    "collectionid: %s, artifactid: %s, committed: %s",
	    collectionid, artifactid, committed);

	log.warn("Parsed request: " + parsedRequest);

	throw new LockssRestServiceException(errorMessage, HttpStatus.NOT_FOUND,
	    parsedRequest);
      }

      log.info(String.format(
	  "Updating commit status for %s (%s -> %s)",
	  artifactid,
	  repo.isArtifactCommitted(artifactid),
	  committed
	  ));

      // Record the commit status in storage
      repo.commitArtifact(collectionid, artifactid);
    } catch (IOException e) {
      String errorMessage = String.format(
	  "IOException occurred while attempting to update artifact metadata (artifactId: %s)",
	  artifactid);

      log.warn(errorMessage, e);

      String parsedRequest = String.format(
	  "collectionid: %s, artifactid: %s, committed: %s",
	  collectionid, artifactid, committed);
   
      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(errorMessage, e,
	  HttpStatus.INTERNAL_SERVER_ERROR, parsedRequest);
    }

    return new ResponseEntity<>(HttpStatus.OK);
  }

  /**
   * POST /collections/{collectionid}/artifacts:
   * Adds artifacts to the repository
   *
   * @param collectionid
   *          A String with the name of the collection containing the artifact.
   * @param auid
   *          A String with the Archival Unit ID (AUID) of new artifact.
   * @param uri
   *          A String with the URI represented by this artifact.
   * @param version
   *          An Integer with the version of the URI contained by the artifact.
   * @param content
   *          A MultipartFile with the artifact content.
   * @param aspectParts
   *          A MultipartFile... with the artifact aspects.
   * @return a {@code ResponseEntity<Artifact>}.
   */
  @Override
  public ResponseEntity<Artifact> collectionsCollectionidArtifactsPost(
      @ApiParam(value = "Collection containing the artifact",required=true)
      @PathVariable("collectionid") String collectionid,
      @ApiParam(value = "Archival Unit ID (AUID) of new artifact", required=true)
      @RequestParam(value="auid", required=true)  String auid,
      @ApiParam(value = "URI represented by this artifact", required=true)
      @RequestParam(value="uri", required=true)  String uri,
      @ApiParam(value = "The version of the URI contained by the artifact", required=true)
      @RequestParam(value="version", required=true)  Integer version,
      @ApiParam(value = "file detail") @Valid
      @RequestPart("file") MultipartFile content,
      @ApiParam(value = "URI aspects represented by this artifact", required=true)
      @RequestParam(value="aspects", required=true)  MultipartFile aspectParts
      ) {

    log.info(String.format("Adding artifact %s, %s, %s, %d",
	collectionid, auid, uri, version));

    try {
      log.info(String.format("MultipartFile: Type: ArtifactData, Content-type: %s",
	  content.getContentType()));

      // Only accept artifact encoded within an HTTP response
      if (!isHttpResponseType(MediaType.parseMediaType(content
	  .getContentType()))) {
	String errorMessage = String.format(
	    "Failed to add artifact; expected %s but got %s",
	    APPLICATION_HTTP_RESPONSE,
	    MediaType.parseMediaType(content.getContentType()));

	log.warn(errorMessage);

	String parsedRequest = String.format(
	    "collectionid: %s, auid: %s, uri: %s, version: %s",
	    collectionid, auid, uri, version);

	log.warn("Parsed request: " + parsedRequest);

	throw new LockssRestServiceException(errorMessage,
	    HttpStatus.BAD_REQUEST, parsedRequest);
      }

      // Convert multipart stream to ArtifactData
      ArtifactData artifactData =
	  ArtifactDataFactory.fromHttpResponseStream(content.getInputStream());

      // Set ArtifactData properties from the POST request
      ArtifactIdentifier id =
	  new ArtifactIdentifier(collectionid, auid, uri, version);
      artifactData.setIdentifier(id);
      artifactData.setContentLength(content.getSize());

      Artifact artifact = repo.addArtifact(artifactData);

      log.info(String.format("Wrote artifact to %s", artifactData.getStorageUrl()));

      // TODO: Process artifact's aspects
//      for (MultipartFile aspectPart : aspectParts) {
//	log.warn(String.format("Ignoring MultipartFile: Type: Aspect, Content-type: %s",
//	    aspectPart.getContentType()));
	//log.info(IOUtils.toString(aspectPart.getInputStream()));
	//log.info(aspectPart.getName());

	/*
                // Augment custom metadata headers with headers from HTTP response
                for (Header header : response.getAllHeaders()) {
                    headers.add(header.getName(), header.getValue());
                }

                // Set content stream and its properties
                artifactMetadata.setContentType(response.getEntity().getContentType().getValue());
                artifactMetadata.setContentLength((int) response.getEntity().getContentLength());
                artifactMetadata.setContentDate(0);
                artifactMetadata.setLastModified(0);
                artifactMetadata.setContentHash(null);

                // Create an artifactIndex
                SolrArtifactIndexData info = artifactStore.addArtifact(artifactMetadata, response.getEntity().getContent());
                artifactIndexRepository.save(info);
	 */
//      }

      return new ResponseEntity<>(artifact, HttpStatus.OK);
    } catch (IOException e) {
      String errorMessage =
	  "Caught IOException while attempting to add an artifact to the repository";

      log.warn(errorMessage, e);

      String parsedRequest = String.format(
	  "collectionid: %s, auid: %s, uri: %s, version: %s",
	  collectionid, auid, uri, version);
   
      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(errorMessage, e,
	  HttpStatus.INTERNAL_SERVER_ERROR, parsedRequest);
    }
  }

  /**
   * GET /collections/{collectionid}/aus/{auid}/artifacts:
   * Get committed artifacts in a collection and Archival Unit.
   *
   * @param collectionid
   *          A String with the name of the collection containing the artifact.
   * @param auid
   *          A String with the Archival Unit ID (AUID) of artifact.
   * @param url
   *          A String with the URL contained by the artifacts.
   * @param urlPrefix
   *          A String with the prefix to be matched by the artifact URLs.
   * @param version
   *          An Integer with the version of the URL contained by the artifacts.
   * @return a {@code ResponseEntity<List<Artifact>>}.
   */
  @Override
  public ResponseEntity<List<Artifact>> collectionsCollectionidAusAuidArtifactsGet(
      @ApiParam(value = "Identifier of the collection containing the artifacts",required=true)
      @PathVariable("collectionid") String collectionid,
      @ApiParam(value = "Identifier of the Archival Unit containing the artifacts",required=true)
      @PathVariable("auid") String auid,
      @ApiParam(value = "The URL contained by the artifacts") @Valid
      @RequestParam(value = "url", required = false) String url,
      @ApiParam(value = "The prefix to be matched by the artifact URLs") @Valid
      @RequestParam(value = "urlPrefix", required = false) String urlPrefix,
      @ApiParam(value = "The version of the URL contained by the artifacts")
      @Valid
      @RequestParam(value = "version", required = false) String version) {
    log.debug("collectionsCollectionidAusAuidGet(): collectionid = "
      + collectionid + ", auid = " + auid + ", url = " + url + ", urlPrefix = "
      + urlPrefix + ", version = " + version);

    boolean isLatestVersion =
	version == null || version.toLowerCase().equals("latest");

    boolean isAllVersions =
	version != null && version.toLowerCase().equals("all");

    if (urlPrefix != null && url != null) {
      String errorMessage =
	  "The 'urlPrefix' and 'url' arguments are mutually exclusive";

      log.warn(errorMessage);

      String parsedRequest = String.format(
	  "collectionid: %s, auid: %s, url: %s, urlPrefix: %s, version: %s",
	  collectionid, auid, url, urlPrefix, version);
     
      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(errorMessage, HttpStatus.BAD_REQUEST,
	  parsedRequest);
    }

    boolean isSpecificVersion = !isAllVersions && !isLatestVersion;
    boolean isAllUrls = url == null && urlPrefix == null;

    if (isSpecificVersion && (isAllUrls || urlPrefix != null)) {
      String errorMessage =
	  "A specific 'version' argument requires a 'url' argument";

      log.warn(errorMessage);

      String parsedRequest = String.format(
	  "collectionid: %s, auid: %s, url: %s, urlPrefix: %s, version: %s",
	  collectionid, auid, url, urlPrefix, version);
     
      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(errorMessage, HttpStatus.BAD_REQUEST,
	  parsedRequest);
    }

    int numericVersion = 0;

    if (isSpecificVersion) {
      try {
	numericVersion = Integer.parseInt(version);

	if (numericVersion <= 0) {
	  String errorMessage =
	      "The 'version' argument is not a positive integer";

	  log.warn(errorMessage);

	  String parsedRequest = String.format(
	      "collectionid: %s, auid: %s, url: %s, urlPrefix: %s, version: %s",
	      collectionid, auid, url, urlPrefix, version);

	  log.warn("Parsed request: " + parsedRequest);

	  throw new LockssRestServiceException(errorMessage,
	      HttpStatus.BAD_REQUEST, parsedRequest);
	}
      } catch (NumberFormatException nfe) {
	String errorMessage =
	    "The 'version' argument is not a positive integer";

	log.warn(errorMessage);

	String parsedRequest = String.format(
	    "collectionid: %s, auid: %s, url: %s, urlPrefix: %s, version: %s",
	    collectionid, auid, url, urlPrefix, version);

	log.warn("Parsed request: " + parsedRequest);

	throw new LockssRestServiceException(errorMessage,
	    HttpStatus.BAD_REQUEST, parsedRequest);
      }
    }

    try {
      List<Artifact> result = new ArrayList<>();

      if (isAllUrls && isAllVersions) {
	log.debug("collectionsCollectionidAusAuidGet(): "
	    + "All versions of all URLs");
	repo.getAllArtifactsAllVersions(collectionid, auid)
	.forEach(result::add);
      } else if (urlPrefix != null && isAllVersions) {
	log.debug("collectionsCollectionidAusAuidGet(): "
	    + "All versions of all URLs matching a prefix");
	repo.getAllArtifactsWithPrefixAllVersions(collectionid, auid,
	    urlPrefix).forEach(result::add);
      } else if (url != null && isAllVersions) {
	log.debug("collectionsCollectionidAusAuidGet(): "
	    + "All versions of a URL");
	repo.getArtifactAllVersions(collectionid, auid, url)
	.forEach(result::add);
      } else if (isAllUrls && isLatestVersion) {
	log.debug("collectionsCollectionidAusAuidGet(): "
	    + "Latest versions of all URLs");
	repo.getAllArtifacts(collectionid, auid).forEach(result::add);
      } else if (urlPrefix != null && isLatestVersion) {
	log.debug("collectionsCollectionidAusAuidGet(): "
	    + "Latest versions of all URLs matching a prefix");
	repo.getAllArtifactsWithPrefix(collectionid, auid, urlPrefix)
	.forEach(result::add);
      } else if (url != null && isLatestVersion) {
	log.debug("collectionsCollectionidAusAuidGet(): "
	    + "Latest version of a URL");
	Artifact artifact = repo.getArtifact(collectionid, auid, url);
	log.debug("collectionsCollectionidAusAuidGet(): artifact = "
	    + artifact);

	if (artifact != null) {
	  result.add(artifact);
	}
      } else if (url != null && numericVersion > 0) {
	log.debug("collectionsCollectionidAusAuidGet(): "
	    + "Given version of a URL");
	Artifact artifact =
	    repo.getArtifactVersion(collectionid, auid, url, numericVersion);
	log.debug("collectionsCollectionidAusAuidGet(): artifact = "
	    + artifact);

	if (artifact != null) {
	  result.add(artifact);
	}
      } else {
	String errorMessage = "The request could not be understood";

	log.warn(errorMessage);

	String parsedRequest = String.format(
	    "collectionid: %s, auid: %s, url: %s, urlPrefix: %s, version: %s",
	    collectionid, auid, url, urlPrefix, version);

	log.warn("Parsed request: " + parsedRequest);

	throw new LockssRestServiceException(errorMessage,
	    HttpStatus.BAD_REQUEST, parsedRequest);
      }

      log.debug("collectionsCollectionidAusAuidGet(): result.size() = "
	  + result.size());

      return new ResponseEntity<>(result, HttpStatus.OK);
    } catch (Exception e) {
      String errorMessage =
	  "Unexpected exception caught while attempting to retrieve artifacts";

	log.warn(errorMessage, e);

	String parsedRequest = String.format(
	    "collectionid: %s, auid: %s, url: %s, urlPrefix: %s, version: %s",
	    collectionid, auid, url, urlPrefix, version);

	log.warn("Parsed request: " + parsedRequest);

	throw new LockssRestServiceException(errorMessage, e,
	    HttpStatus.INTERNAL_SERVER_ERROR, parsedRequest);
    }
  }

  /**
   * GET /collections/{collectionid}/aus/{auid}/size:
   * Get the size of Archival Unit artifacts in a collection.
   *
   * @param collectionid
   *          A String with the name of the collection containing the Archival
   *          Unit.
   * @param auid
   *          A String with the Archival Unit ID (AUID).
   * @param url
   *          A String with the URL contained by the artifacts.
   * @param urlPrefix
   *          A String with the prefix to be matched by the artifact URLs.
   * @param version
   *          An Integer with the version of the URL contained by the artifacts.
   * @return a Long{@code ResponseEntity<Long>}.
   */
  @Override
  public ResponseEntity<Long> collectionsCollectionidAusAuidSizeGet(
      @ApiParam(value = "Identifier of the collection containing the artifacts",required=true)
      @PathVariable("collectionid") String collectionid,
      @ApiParam(value = "Identifier of the Archival Unit containing the artifacts",required=true)
      @PathVariable("auid") String auid,
      @ApiParam(value = "The URL contained by the artifacts") @Valid
      @RequestParam(value = "url", required = false) String url,
      @ApiParam(value = "The prefix to be matched by the artifact URLs") @Valid
      @RequestParam(value = "urlPrefix", required = false) String urlPrefix,
      @ApiParam(value = "The version of the URL contained by the artifacts")
      @Valid
      @RequestParam(value = "version", required = false) String version) {
    log.debug("collectionsCollectionidAusAuidSizeGet(): "
	+ collectionid + ", auid = " + auid + ", url = " + url
	+ ", urlPrefix = " + urlPrefix + ", version = " + version);

    try {
      Long result = repo.auSize(collectionid, auid);
      log.debug("collectionsCollectionidAusAuidSizeGet(): result = " + result);
      return new ResponseEntity<>(result, HttpStatus.OK);
    } catch (Exception e) {
      String errorMessage =
	  "Unexpected exception caught while attempting to get artifacts size";

      log.warn(errorMessage, e);

      String parsedRequest = String.format(
	  "collectionid: %s, auid: %s, url: %s, urlPrefix: %s, version: %s",
	  collectionid, auid, url, urlPrefix, version);

      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(errorMessage, e,
	  HttpStatus.INTERNAL_SERVER_ERROR, parsedRequest);
    }
  }

  /**
   * GET /collections/{collectionid}/aus:
   * Get Archival Unit IDs (AUIDs) in a collection.
   *
   * @param collectionid
   *          A String with the name of the collection containing the artifact.
   * @return a {@code ResponseEntity<List<String>>}.
   */
  @Override
  public ResponseEntity<List<String>> collectionsCollectionidAusGet(
      @ApiParam(value = "Identifier of the collection containing the Archival Units", required = true)
      @PathVariable("collectionid") String collectionid) {
    log.debug("collectionsCollectionidAusGet(): collectionid = "
      + collectionid);

    try {
      List<String> result = new ArrayList<>();
      repo.getAuIds(collectionid).forEach(result::add);
      log.debug("collectionsCollectionidAusGet(): result.size() = "
	  + result.size());
      return new ResponseEntity<>(result, HttpStatus.OK);
    } catch (Exception e) {
      String errorMessage =
	  "Unexpected exception caught while attempting to get AU ids";

      log.warn(errorMessage, e);

      String parsedRequest = String.format("collectionid: %s", collectionid);

      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(errorMessage, e,
	  HttpStatus.INTERNAL_SERVER_ERROR, parsedRequest);
    }
  }

  private static Boolean isHttpResponseType(MediaType type) {
    return (APPLICATION_HTTP_RESPONSE.isCompatibleWith(type) && (type
	.getParameters().equals(APPLICATION_HTTP_RESPONSE.getParameters())));
  }

  @Override
  public Optional<ObjectMapper> getObjectMapper() {
    return Optional.ofNullable(objectMapper);
  }

  @Override
  public Optional<HttpServletRequest> getRequest() {
    return Optional.ofNullable(request);
  }
}
