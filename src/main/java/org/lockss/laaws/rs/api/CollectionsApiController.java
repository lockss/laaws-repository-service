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
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@Controller
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
    log.info("collectionsGet() invoked");
    List<String> collectionIds = new ArrayList<>();
    try {
      repo.getCollectionIds().forEach(x -> collectionIds.add(x));
    } catch (IOException e) {
      log.error("IOException was caught trying to enumerate collection IDs");
      return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    log.info("collectionIds = " + collectionIds);
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
      if (!repo.artifactExists(artifactid))
	return new ResponseEntity<>(HttpStatus.NOT_FOUND);

      // Remove the artifact from the artifact store and index
      repo.deleteArtifact(collectionid, artifactid);
      return new ResponseEntity<>(HttpStatus.OK);

    } catch (IOException e) {
      log.error(String.format(
	  "IOException occurred while attempting to delete artifact from repository (artifactId: %s)",
	  artifactid
	  ));

      return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
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
      if (!repo.artifactExists(artifactid))
	return new ResponseEntity<>(HttpStatus.NOT_FOUND);

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
      log.error(String.format(
	  "IOException occurred while attempting to retrieve artifact from repository (artifactId: %s): %s",
	  artifactid,
	  e
	  ));

      return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
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
    if (committed == null)
      return new ResponseEntity<>(HttpStatus.BAD_REQUEST);

    try {
      // Make sure that the artifact exists
      if (!repo.artifactExists(artifactid))
	return new ResponseEntity<>(HttpStatus.NOT_FOUND);

      log.info(String.format(
	  "Updating commit status for %s (%s -> %s)",
	  artifactid,
	  repo.isArtifactCommitted(artifactid),
	  committed
	  ));

      // Record the commit status in storage
      repo.commitArtifact(collectionid, artifactid);
    } catch (IOException e) {
      log.error(String.format(
	  "IOException occurred while attempting to update artifact metadata (artifactId: %s): %s",
	  artifactid,
	  e
	  ));

      return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
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

	log.error(String.format("Failed to add artifact; expected %s but got %s",
	    APPLICATION_HTTP_RESPONSE,
	    MediaType.parseMediaType(content.getContentType())));

	return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
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
      log.error("Caught IOException while attempting to add an artifact to the repository");
    }

    return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  /**
   * GET /collections/{collectionid}/aus/{auid}:
   * Get committed artifacts of all versions of all URLs in a collection and
   * Archival Unit.
   *
   * @param collectionid
   *          A String with the name of the collection containing the artifact.
   * @param auid
   *          A String with the Archival Unit ID (AUID) of artifact.
   * @return a {@code ResponseEntity<List<Artifact>>}.
   */
  @Override
  public ResponseEntity<List<Artifact>> collectionsCollectionidAusAuidGet(
      @ApiParam(value = "Identifier of the collection containing the artifacts",required=true)
      @PathVariable("collectionid") String collectionid,
      @ApiParam(value = "Identifier of the Archival Unit containing the artifacts",required=true)
      @PathVariable("auid") String auid) {
    log.info("collectionsCollectionidAusAuidGet() invoked");

    try {
      List<Artifact> result = new ArrayList<>();
      repo.getAllArtifactsAllVersions(collectionid, auid).forEach(result::add);
      log.info("collectionsCollectionidAusAuidGet(): result.size() = "
	  + result.size());
      return new ResponseEntity<>(result, HttpStatus.OK);
    } catch (Exception e) {
      log.error(String.format("Exception occurred while attempting to retrieve"
	  + " artifacts from repository (collectionid: %s, auid: %s): %s",
	  collectionid, auid, e));
    }

    return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  /**
   * GET /collections/{collectionid}/aus/{auid}/url-prefix/{prefix}:
   * Get committed artifacts of all versions of all URLs matching a prefix, in a
   * collection and Archival Unit.
   *
   * @param collectionid
   *          A String with the name of the collection containing the artifact.
   * @param auid
   *          A String with the Archival Unit ID (AUID) of artifact.
   * @param prefix
   *          A String with the URL prefix.
   * @return a {@code ResponseEntity<List<Artifact>>}.
   */
  @Override
  public ResponseEntity<List<Artifact>> collectionsCollectionidAusAuidUrlPrefixPrefixGet(
      @ApiParam(value = "Identifier of the collection containing the artifacts", required = true)
      @PathVariable("collectionid") String collectionid,
      @ApiParam(value = "Identifier of the Archival Unit containing the artifacts", required = true)
      @PathVariable("auid") String auid,
      @ApiParam(value = "The prefix to be matched by the artifact URLs", required = true)
      @PathVariable("prefix") String prefix) {
    log.info("collectionsCollectionidAusAuidUrlPrefixPrefixGet() invoked");

    try {
      List<Artifact> result = new ArrayList<>();
      repo.getAllArtifactsWithPrefixAllVersions(collectionid, auid, prefix)
      .forEach(result::add);
      log.info("collectionsCollectionidAusAuidUrlPrefixPrefixGet(): result.size() = "
	  + result.size());
      return new ResponseEntity<>(result, HttpStatus.OK);
    } catch (Exception e) {
      log.error(String.format("Exception occurred while attempting to retrieve"
	  + " artifacts from repository (collectionid: %s, auid: %s,"
	  + " prefix: %s): %s", collectionid, auid, prefix, e));
    }

    return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  /**
   * GET /collections/{collectionid}/aus/{auid}/url-prefix/{prefix}/latest:
   * Get committed artifacts of the latest version of all URLs matching a
   * prefix, in a collection and Archival Unit.
   *
   * @param collectionid
   *          A String with the name of the collection containing the artifact.
   * @param auid
   *          A String with the Archival Unit ID (AUID) of artifact.
   * @param prefix
   *          A String with the URL prefix.
   * @return a {@code ResponseEntity<List<Artifact>>}.
   */
  @Override
  public ResponseEntity<List<Artifact>> collectionsCollectionidAusAuidUrlPrefixPrefixLatestGet(
      @ApiParam(value = "Identifier of the collection containing the artifacts", required = true)
      @PathVariable("collectionid") String collectionid,
      @ApiParam(value = "Identifier of the Archival Unit containing the artifacts", required = true)
      @PathVariable("auid") String auid,
      @ApiParam(value = "The prefix to be matched by the artifact URLs", required = true)
      @PathVariable("prefix") String prefix) {
    log.info("collectionsCollectionidAusAuidUrlPrefixPrefixLatestGet() invoked");

    try {
      List<Artifact> result = new ArrayList<>();
      repo.getAllArtifactsWithPrefix(collectionid, auid, prefix)
      .forEach(result::add);
      log.info("collectionsCollectionidAusAuidUrlPrefixPrefixLatestGet(): result.size() = "
	  + result.size());
      return new ResponseEntity<>(result, HttpStatus.OK);
    } catch (Exception e) {
      log.error(String.format("Exception occurred while attempting to retrieve"
	  + " artifacts from repository (collectionid: %s, auid: %s,"
	  + " prefix: %s): %s", collectionid, auid, prefix, e));
    }

    return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  /**
   * GET /collections/{collectionid}/aus/{auid}/urls:
   * Get committed artifacts of the latest version of all URLs in a collection
   * and Archival Unit.
   *
   * @param collectionid
   *          A String with the name of the collection containing the artifact.
   * @param auid
   *          A String with the Archival Unit ID (AUID) of artifact.
   * @return a {@code ResponseEntity<List<Artifact>>}.
   */
  @Override
  public ResponseEntity<List<Artifact>> collectionsCollectionidAusAuidUrlsGet(
      @ApiParam(value = "Identifier of the collection containing the artifacts", required = true)
      @PathVariable("collectionid") String collectionid,
      @ApiParam(value = "Identifier of the Archival Unit containing the artifacts", required = true)
      @PathVariable("auid") String auid) {
    log.info("collectionsCollectionidAusAuidUrlsGet() invoked");

    try {
      List<Artifact> result = new ArrayList<>();
      repo.getAllArtifacts(collectionid, auid).forEach(result::add);
      log.info("collectionsCollectionidAusAuidUrlsGet(): result.size() = "
	  + result.size());
      return new ResponseEntity<>(result, HttpStatus.OK);
    } catch (Exception e) {
      log.error(String.format("Exception occurred while attempting to retrieve"
	  + " artifacts from repository (collectionid: %s, auid: %s): %s",
	  collectionid, auid, e));
    }

    return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  /**
   * GET /collections/{collectionid}/aus/{auid}/urls/{url}:
   * Get committed artifacts of all versions of a given URL in a collection and
   * Archival Unit.
   *
   * @param collectionid
   *          A String with the name of the collection containing the artifact.
   * @param auid
   *          A String with the Archival Unit ID (AUID) of artifact.
   * @param url
   *          A String with the URL.
   * @return a {@code ResponseEntity<List<Artifact>>}.
   */
  @Override
  public ResponseEntity<List<Artifact>> collectionsCollectionidAusAuidUrlsUrlGet(
      @ApiParam(value = "Identifier of the collection containing the artifacts", required = true)
      @PathVariable("collectionid") String collectionid,
      @ApiParam(value = "Identifier of the Archival Unit containing the artifacts", required = true)
      @PathVariable("auid") String auid,
      @ApiParam(value = "The URL contained by the artifacts", required = true)
      @PathVariable("url") String url) {
    log.info("collectionsCollectionidAusAuidUrlsUrlGet() invoked");

    try {
      List<Artifact> result = new ArrayList<>();
      repo.getArtifactAllVersions(collectionid, auid, url)
      .forEach(result::add);
      log.info("collectionsCollectionidAusAuidUrlsUrlGet(): result.size() = "
	  + result.size());
      return new ResponseEntity<>(result, HttpStatus.OK);
    } catch (Exception e) {
      log.error(String.format("Exception occurred while attempting to retrieve"
	  + " artifacts from repository (collectionid: %s, auid: %s, url: %s):"
	  + " %s", collectionid, auid, url, e));
    }

    return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  /**
   * GET /collections/{collectionid}/aus/{auid}/urls/{url}/latest:
   * Get the committed artifact with the latest version of a given URL in a
   * collection and Archival Unit.
   *
   * @param collectionid
   *          A String with the name of the collection containing the artifact.
   * @param auid
   *          A String with the Archival Unit ID (AUID) of artifact.
   * @param url
   *          A String with the URL.
   * @return a {@code ResponseEntity<Artifact>}.
   */
  @Override
  public ResponseEntity<Artifact> collectionsCollectionidAusAuidUrlsUrlLatestGet(
      @ApiParam(value = "Identifier of the collection containing the artifact", required = true)
      @PathVariable("collectionid") String collectionid,
      @ApiParam(value = "Identifier of the Archival Unit containing the artifact", required = true)
      @PathVariable("auid") String auid,
      @ApiParam(value = "The URL contained by the artifact", required = true)
      @PathVariable("url") String url) {
    log.info("collectionsCollectionidAusAuidUrlsUrlLatestGet() invoked");

    try {
      Artifact result = repo.getArtifact(collectionid, auid, url);
      log.info("collectionsCollectionidAusAuidUrlsUrlLatestGet(): result " + result);
      return new ResponseEntity<>(result, HttpStatus.OK);
    } catch (Exception e) {
      log.error(String.format("Exception occurred while attempting to retrieve"
	  + " artifact from repository (collectionid: %s, auid: %s, url: %s):"
	  + " %s", collectionid, auid, url, e));
    }

    return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
  }

  /**
   * Implementation of GET
   * /collections/{collectionid}/aus/{auid}/urls/{url}/{version}: Get the
   * committed artifact with a given version of a given URL in a collection and
   * Archival Unit.
   *
   * @param collectionid
   *          A String with the name of the collection containing the artifact.
   * @param auid
   *          A String with the Archival Unit ID (AUID) of artifact.
   * @param url
   *          A String with the URL.
   * @param version
   *          An Integer with the version.
   * @return a {@code ResponseEntity<Artifact>}.
   */
  @Override
  public ResponseEntity<Artifact> collectionsCollectionidAusAuidUrlsUrlVersionGet(
      @ApiParam(value = "Identifier of the collection containing the artifact", required = true)
      @PathVariable("collectionid") String collectionid,
      @ApiParam(value = "Identifier of the Archival Unit containing the artifact", required = true)
      @PathVariable("auid") String auid,
      @ApiParam(value = "The URL contained by the artifact", required = true)
      @PathVariable("url") String url,
      @ApiParam(value = "The version of the URL contained by the artifact", required = true)
      @PathVariable("version") Integer version) {
    log.info("collectionsCollectionidAusAuidUrlsUrlVersionGet() invoked");

    try {
      Artifact result =
	  repo.getArtifactVersion(collectionid, auid, url, version);
      log.info("collectionsCollectionidAusAuidUrlsUrlVersionGet(): result " + result);
      return new ResponseEntity<>(result, HttpStatus.OK);
    } catch (Exception e) {
      log.error(String.format("Exception occurred while attempting to retrieve"
	  + " artifact from repository (collectionid: %s, auid: %s, url: %s, version: %i):"
	  + " %s", collectionid, auid, url, version, e));
    }

    return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
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
    log.info("collectionsCollectionidAusGet() invoked");

    try {
      List<String> result = new ArrayList<>();
      repo.getAuIds(collectionid).forEach(result::add);
      log.info("collectionsCollectionidAusGet(): result.size() = "
	  + result.size());
      return new ResponseEntity<>(result, HttpStatus.OK);
    } catch (Exception e) {
      log.error(String.format("Exception occurred while attempting to retrieve"
	  + " artifacts from repository (collectionid: %s): %s", collectionid,
	  e));
    }

    return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
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
