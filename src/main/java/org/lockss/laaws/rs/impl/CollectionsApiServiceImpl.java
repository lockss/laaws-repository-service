/*
 * Copyright (c) 2017-2019, Board of Trustees of Leland Stanford Jr. University,
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

package org.lockss.laaws.rs.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lockss.laaws.error.LockssRestServiceException;
import org.lockss.laaws.rs.api.CollectionsApiDelegate;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.util.ArtifactConstants;
import org.lockss.laaws.rs.util.ArtifactDataFactory;
import org.lockss.laaws.rs.util.ArtifactDataUtil;
import org.lockss.laaws.status.model.ApiStatus;
import org.lockss.spring.status.SpringLockssBaseApiController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

/**
 * Service for accessing the repository artifacts.
 */
@Service
public class CollectionsApiServiceImpl extends SpringLockssBaseApiController
    implements CollectionsApiDelegate {
  private final static Log log =
      LogFactory.getLog(CollectionsApiServiceImpl.class);
  private static final String APPLICATION_HTTP_RESPONSE_VALUE =
      "application/http;msgtype=response";
  private static final MediaType APPLICATION_HTTP_RESPONSE =
      MediaType.parseMediaType(APPLICATION_HTTP_RESPONSE_VALUE);

  @Autowired
  LockssRepository repo;

  private final ObjectMapper objectMapper;

  private final HttpServletRequest request;

  /**
   * Constructor for autowiring.
   * 
   * @param objectMapper
   *          An ObjectMapper for JSON processing.
   * @param request
   *          An HttpServletRequest with the HTTP request.
   */
  @org.springframework.beans.factory.annotation.Autowired
  public CollectionsApiServiceImpl(ObjectMapper objectMapper,
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
  public ResponseEntity<List<String>> getCollections() {
    String parsedRequest = String.format("requestUrl: %s",
	ServiceImplUtil.getFullRequestUrl(request));
    log.debug("Parsed request: " + parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      log.debug("Invoked");
      List<String> collectionIds = new ArrayList<>();
      Iterable<String> ids = repo.getCollectionIds();
      log.debug("ids.iterator().hasNext() = " + ids.iterator().hasNext());
      ids.forEach(x -> collectionIds.add(x));

      log.debug("collectionIds = " + collectionIds);
      return new ResponseEntity<>(collectionIds, HttpStatus.OK);
    } catch (IOException e) {
      String errorMessage =
	  "IOException was caught trying to enumerate collection IDs";

      log.warn(errorMessage, e);
      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(HttpStatus.INTERNAL_SERVER_ERROR,
	  errorMessage, e, parsedRequest);
    }
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
  public ResponseEntity<Void> deleteArtifact(String collectionid,
      String artifactid) {
    String parsedRequest = String.format(
	"collectionid: %s, artifactid: %s, requestUrl: %s",
	collectionid, artifactid, ServiceImplUtil.getFullRequestUrl(request));
    log.debug("Parsed request: " + parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      // Check that the collection exists.
//      validateCollectionId(collectionid, parsedRequest);

      // Check that the artifact exists
      validateArtifactExists(collectionid, artifactid,
	  "The artifact to be deleted does not exist", parsedRequest);

      // Remove the artifact from the artifact store and index
      repo.deleteArtifact(collectionid, artifactid);
      return new ResponseEntity<>(HttpStatus.OK);
    } catch (LockssRestServiceException lre) {
      // Let it cascade to the controller advice exception handler.
      throw lre;
    } catch (IOException e) {
      String errorMessage = String.format(
	  "IOException occurred while attempting to delete artifact from repository (artifactId: %s)",
	  artifactid);

      log.warn(errorMessage, e);
      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(HttpStatus.INTERNAL_SERVER_ERROR,
	  errorMessage, e, parsedRequest);
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
  public ResponseEntity<StreamingResponseBody> getArtifact(String collectionid,
      String artifactid, String accept) {
    String parsedRequest = String.format(
	"collectionid: %s, artifactid: %s, accept: %s, requestUrl: %s",
	collectionid, artifactid, accept,
	ServiceImplUtil.getFullRequestUrl(request));
    log.debug("Parsed request: " + parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      log.info(String.format("Retrieving artifact: %s from collection %s",
		artifactid, collectionid));

      // Check that the collection exists.
//      validateCollectionId(collectionid, parsedRequest);

      // Check that the artifact exists
      validateArtifactExists(collectionid, artifactid,
	  "The artifact to be retrieved does not exist", parsedRequest);

      // Retrieve the ArtifactData from the artifact store
      ArtifactData artifactData =
	  repo.getArtifactData(collectionid, artifactid);

      // Setup HTTP response headers
      HttpHeaders headers = new HttpHeaders();
      headers.setContentType(
	  MediaType.parseMediaType("application/http; msgtype=response"));

      // TODO: Set to content length of the HTTP response entity body (i.e., the HTTP response encoding the artifact)
//      headers.setContentLength(artifactData.getContentLength());

      // Include LOCKSS repository headers in the HTTP response
      ArtifactIdentifier id = artifactData.getIdentifier();
      headers.set(ArtifactConstants.ARTIFACT_ID_KEY, id.getId());
      headers.set(ArtifactConstants.ARTIFACT_COLLECTION_KEY,
	  id.getCollection());
      headers.set(ArtifactConstants.ARTIFACT_AUID_KEY, id.getAuid());
      headers.set(ArtifactConstants.ARTIFACT_URI_KEY, id.getUri());
      headers.set(ArtifactConstants.ARTIFACT_VERSION_KEY,
	  String.valueOf(id.getVersion()));

      headers.set(
              ArtifactConstants.ARTIFACT_STATE_COMMITTED,
              String.valueOf(artifactData.getRepositoryMetadata().getCommitted())
      );

      headers.set(
              ArtifactConstants.ARTIFACT_STATE_DELETED,
              String.valueOf(artifactData.getRepositoryMetadata().getDeleted())
      );

      headers.set(ArtifactConstants.ARTIFACT_LENGTH_KEY, String.valueOf(artifactData.getContentLength()));
      headers.set(ArtifactConstants.ARTIFACT_DIGEST_KEY, artifactData.getContentDigest());

      return new ResponseEntity<>(
              outputStream -> ArtifactDataUtil.writeHttpResponse(
                      ArtifactDataUtil.getHttpResponseFromArtifactData(artifactData),
                      outputStream
              ),
              headers,
              HttpStatus.OK
      );
    } catch (LockssRestServiceException lre) {
      // Let it cascade to the controller advice exception handler.
      throw lre;
    } catch (IOException e) {
      String errorMessage = String.format(
	  "IOException occurred while attempting to retrieve artifact from repository (artifactId: %s)",
	  artifactid);

      log.warn(errorMessage, e);
      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(HttpStatus.INTERNAL_SERVER_ERROR,
	  errorMessage, e, parsedRequest);
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
  public ResponseEntity<Artifact> updateArtifact(String collectionid,
      String artifactid, Boolean committed) {
    String parsedRequest = String.format(
	"collectionid: %s, artifactid: %s, committed: %s, requestUrl: %s",
	collectionid, artifactid, committed,
	ServiceImplUtil.getFullRequestUrl(request));
    log.debug("Parsed request: " + parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      // Return bad request if new commit status has not been passed
      if (committed == null) {
	String errorMessage = "The committed status cannot be null";
	log.warn(errorMessage);
	log.warn("Parsed request: " + parsedRequest);

	throw new LockssRestServiceException(HttpStatus.BAD_REQUEST,
	    errorMessage, parsedRequest);
      }

      // Check that the collection exists.
//      validateCollectionId(collectionid, parsedRequest);

      // Check that the artifact exists
      validateArtifactExists(collectionid, artifactid,
	  "The artifact to be updated does not exist", parsedRequest);

      log.info(String.format(
	  "Updating commit status for %s (%s -> %s)",
	  artifactid,
	  repo.isArtifactCommitted(collectionid, artifactid),
	  committed
	  ));

      // Record the commit status in storage and return the new representation in the response entity body
      Artifact updatedArtifact = repo.commitArtifact(collectionid, artifactid);
      return new ResponseEntity<>(updatedArtifact, HttpStatus.OK);
    } catch (LockssRestServiceException lre) {
      // Let it cascade to the controller advice exception handler.
      throw lre;
    } catch (IOException e) {
      String errorMessage = String.format(
	  "IOException occurred while attempting to update artifact metadata (artifactId: %s)",
	  artifactid);

      log.warn(errorMessage, e);
      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(HttpStatus.INTERNAL_SERVER_ERROR,
	  errorMessage, e, parsedRequest);
    }
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
   * @param content
   *          A MultipartFile with the artifact content.
   * @param aspectParts
   *          A MultipartFile... with the artifact aspects.
   * @return a {@code ResponseEntity<Artifact>}.
   */
  @Override
  public ResponseEntity<Artifact> createArtifact(String collectionid,
      String auid, String uri, MultipartFile content, MultipartFile aspectParts)
  {
    String parsedRequest = String.format(
	"collectionid: %s, auid: %s, uri: %s, requestUrl: %s",
	collectionid, auid, uri, ServiceImplUtil.getFullRequestUrl(request));
    log.debug("Parsed request: " + parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      log.info(String.format("Adding artifact %s, %s, %s",
		collectionid, auid, uri));

      log.info(String.format("MultipartFile: Type: ArtifactData, Content-type: %s",
	  content.getContentType()));

      // Check URI.
      validateUri(uri, parsedRequest);

      // Only accept artifact encoded within an HTTP response
      if (!isHttpResponseType(MediaType.parseMediaType(content
	  .getContentType()))) {
	String errorMessage = String.format(
	    "Failed to add artifact; expected %s but got %s",
	    APPLICATION_HTTP_RESPONSE,
	    MediaType.parseMediaType(content.getContentType()));

	log.warn(errorMessage);
	log.warn("Parsed request: " + parsedRequest);

	throw new LockssRestServiceException(HttpStatus.BAD_REQUEST,
	    errorMessage, parsedRequest);
      }

      // Convert multipart stream to ArtifactData
      ArtifactData artifactData =
	  ArtifactDataFactory.fromHttpResponseStream(content.getInputStream());

      // Set ArtifactData properties from the POST request
//TODO: FIX THIS CALL
      ArtifactIdentifier id =
	  new ArtifactIdentifier(collectionid, auid, uri, 0);
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
    } catch (LockssRestServiceException lre) {
      // Let it cascade to the controller advice exception handler.
      throw lre;
    } catch (IOException e) {
      String errorMessage =
	  "Caught IOException while attempting to add an artifact to the repository";

      log.warn(errorMessage, e);
      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(HttpStatus.INTERNAL_SERVER_ERROR,
	  errorMessage, e, parsedRequest);
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
  public ResponseEntity<List<Artifact>> getCommittedArtifacts(
      String collectionid, String auid, String url, String urlPrefix,
      String version) {
    String parsedRequest = String.format(
	"collectionid: %s, auid: %s, url: %s, urlPrefix: %s, version: %s, requestUrl: %s",
	collectionid, auid, url, urlPrefix, version,
	ServiceImplUtil.getFullRequestUrl(request));
    log.debug("Parsed request: " + parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      boolean isLatestVersion =
	  version == null || version.toLowerCase().equals("latest");

      boolean isAllVersions =
	  version != null && version.toLowerCase().equals("all");

      if (urlPrefix != null && url != null) {
	String errorMessage =
	    "The 'urlPrefix' and 'url' arguments are mutually exclusive";

	log.warn(errorMessage);
	log.warn("Parsed request: " + parsedRequest);

	throw new LockssRestServiceException(HttpStatus.BAD_REQUEST,
	    errorMessage, parsedRequest);
      }

      boolean isSpecificVersion = !isAllVersions && !isLatestVersion;
      boolean isAllUrls = url == null && urlPrefix == null;

      if (isSpecificVersion && (isAllUrls || urlPrefix != null)) {
	String errorMessage =
	    "A specific 'version' argument requires a 'url' argument";

	log.warn(errorMessage);
	log.warn("Parsed request: " + parsedRequest);

	throw new LockssRestServiceException(HttpStatus.BAD_REQUEST,
	    errorMessage, parsedRequest);
      }

      int numericVersion = 0;

      if (isSpecificVersion) {
	try {
	  numericVersion = Integer.parseInt(version);

	  if (numericVersion <= 0) {
	    String errorMessage =
		"The 'version' argument is not a positive integer";

	    log.warn(errorMessage);
	    log.warn("Parsed request: " + parsedRequest);

	    throw new LockssRestServiceException(HttpStatus.BAD_REQUEST,
		errorMessage, parsedRequest);
	  }
	} catch (NumberFormatException nfe) {
	  String errorMessage =
	      "The 'version' argument is not a positive integer";

	  log.warn(errorMessage);
	  log.warn("Parsed request: " + parsedRequest);

	  throw new LockssRestServiceException(HttpStatus.BAD_REQUEST,
	      errorMessage, parsedRequest);
	}
      }

      // Check that the collection exists.
      ServiceImplUtil.validateCollectionId(repo, collectionid, parsedRequest);

      // Check that the Archival Unit exists.
      validateAuId(collectionid, auid, parsedRequest);

      List<Artifact> result = new ArrayList<>();

      if (isAllUrls && isAllVersions) {
	log.debug("collectionsCollectionidAusAuidGet(): "
	    + "All versions of all URLs");
	repo.getArtifactsAllVersions(collectionid, auid)
	.forEach(result::add);
      } else if (urlPrefix != null && isAllVersions) {
	log.debug("collectionsCollectionidAusAuidGet(): "
	    + "All versions of all URLs matching a prefix");
	repo.getArtifactsWithPrefixAllVersions(collectionid, auid, urlPrefix)
	.forEach(result::add);
      } else if (url != null && isAllVersions) {
	log.debug("collectionsCollectionidAusAuidGet(): "
	    + "All versions of a URL");
	repo.getArtifactsAllVersions(collectionid, auid, url)
	.forEach(result::add);
      } else if (isAllUrls && isLatestVersion) {
	log.debug("collectionsCollectionidAusAuidGet(): "
	    + "Latest versions of all URLs");
	repo.getArtifacts(collectionid, auid).forEach(result::add);
      } else if (urlPrefix != null && isLatestVersion) {
	log.debug("collectionsCollectionidAusAuidGet(): "
	    + "Latest versions of all URLs matching a prefix");
	repo.getArtifactsWithPrefix(collectionid, auid, urlPrefix)
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
	log.warn("Parsed request: " + parsedRequest);

	throw new LockssRestServiceException(HttpStatus.BAD_REQUEST,
	    errorMessage, parsedRequest);
      }

      log.debug("collectionsCollectionidAusAuidGet(): result.size() = "
	  + result.size());

      return new ResponseEntity<>(result, HttpStatus.OK);
    } catch (LockssRestServiceException lre) {
      // Let it cascade to the controller advice exception handler.
      throw lre;
    } catch (Exception e) {
      String errorMessage =
	  "Unexpected exception caught while attempting to retrieve artifacts";

	log.warn(errorMessage, e);
	log.warn("Parsed request: " + parsedRequest);

	throw new LockssRestServiceException(HttpStatus.INTERNAL_SERVER_ERROR,
	    errorMessage, e, parsedRequest);
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
  public ResponseEntity<Long> getArtifactsSize(String collectionid, String auid,
      String url, String urlPrefix, String version) {
    String parsedRequest = String.format(
	"collectionid: %s, auid: %s, url: %s, urlPrefix: %s, version: %s, requestUrl: %s",
	collectionid, auid, url, urlPrefix, version,
	ServiceImplUtil.getFullRequestUrl(request));
    log.debug("Parsed request: " + parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      boolean isLatestVersion =
	  version == null || version.toLowerCase().equals("latest");

      boolean isAllVersions =
	  version != null && version.toLowerCase().equals("all");

      if (urlPrefix != null && url != null) {
	String errorMessage =
	    "The 'urlPrefix' and 'url' arguments are mutually exclusive";

	log.warn(errorMessage);
	log.warn("Parsed request: " + parsedRequest);

	throw new LockssRestServiceException(HttpStatus.BAD_REQUEST,
	    errorMessage, parsedRequest);
      }

      boolean isSpecificVersion = !isAllVersions && !isLatestVersion;
      boolean isAllUrls = url == null && urlPrefix == null;

      if (isSpecificVersion && (isAllUrls || urlPrefix != null)) {
	String errorMessage =
	    "A specific 'version' argument requires a 'url' argument";

	log.warn(errorMessage);
	log.warn("Parsed request: " + parsedRequest);

	throw new LockssRestServiceException(HttpStatus.BAD_REQUEST,
	    errorMessage, parsedRequest);
      }

      int numericVersion = 0;

      if (isSpecificVersion) {
	try {
	  numericVersion = Integer.parseInt(version);

	  if (numericVersion <= 0) {
	    String errorMessage =
		"The 'version' argument is not a positive integer";

	    log.warn(errorMessage);
	    log.warn("Parsed request: " + parsedRequest);

	    throw new LockssRestServiceException(HttpStatus.BAD_REQUEST,
		errorMessage, parsedRequest);
	  }
	} catch (NumberFormatException nfe) {
	  String errorMessage =
	      "The 'version' argument is not a positive integer";

	  log.warn(errorMessage);
	  log.warn("Parsed request: " + parsedRequest);

	  throw new LockssRestServiceException(HttpStatus.BAD_REQUEST,
	      errorMessage, parsedRequest);
	}
      }

      // Check that the collection exists.
      ServiceImplUtil.validateCollectionId(repo, collectionid, parsedRequest);

      // Check that the Archival Unit exists.
      validateAuId(collectionid, auid, parsedRequest);

      Long result = repo.auSize(collectionid, auid);
      log.debug("collectionsCollectionidAusAuidSizeGet(): result = " + result);
      return new ResponseEntity<>(result, HttpStatus.OK);
    } catch (LockssRestServiceException lre) {
      // Let it cascade to the controller advice exception handler.
      throw lre;
    } catch (Exception e) {
      String errorMessage =
	  "Unexpected exception caught while attempting to get artifacts size";

      log.warn(errorMessage, e);
      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(HttpStatus.INTERNAL_SERVER_ERROR,
	  errorMessage, e, parsedRequest);
    }
  }

  /**
   * GET /collections/{collectionid}/aus: Get Archival Unit IDs (AUIDs) in a
   * collection.
   *
   * @param collectionid
   *          A String with the name of the collection containing the archival
   *          units.
   * @return a {@code ResponseEntity<List<String>>}.
   */
  @Override
  public ResponseEntity<List<String>> getAus(String collectionid) {
    String parsedRequest = String.format("collectionid: %s, requestUrl: %s",
	collectionid, ServiceImplUtil.getFullRequestUrl(request));
    log.debug("Parsed request: " + parsedRequest);

    ServiceImplUtil.checkRepositoryReady(repo, parsedRequest);

    try {
      // Check that the collection exists.
      ServiceImplUtil.validateCollectionId(repo, collectionid, parsedRequest);

      List<String> result = new ArrayList<>();
      repo.getAuIds(collectionid).forEach(result::add);
      log.debug("collectionsCollectionidAusGet(): result.size() = "
	  + result.size());
      return new ResponseEntity<>(result, HttpStatus.OK);
    } catch (LockssRestServiceException lre) {
      // Let it cascade to the controller advice exception handler.
      throw lre;
    } catch (Exception e) {
      String errorMessage =
	  "Unexpected exception caught while attempting to get AU ids";

      log.warn(errorMessage, e);
      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(HttpStatus.INTERNAL_SERVER_ERROR,
	  errorMessage, e, parsedRequest);
    }
  }

  private static Boolean isHttpResponseType(MediaType type) {
    return (APPLICATION_HTTP_RESPONSE.isCompatibleWith(type) && (type
	.getParameters().equals(APPLICATION_HTTP_RESPONSE.getParameters())));
  }

  private void validateAuId(String collectionid, String auid,
      String parsedRequest) throws IOException {
    if (!StreamSupport.stream(repo.getAuIds(collectionid).spliterator(), false)
	.anyMatch(name -> auid.equals(name))) {
      String errorMessage = "The archival unit does not exist";
      log.warn(errorMessage);
      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(HttpStatus.NOT_FOUND, errorMessage, 
	  parsedRequest);
    }

    log.debug("auid '" + auid + "' is valid.");
  }

  private void validateUri(String uri, String parsedRequest) {
    if (uri.isEmpty()) {
      String errorMessage = "The URI has not been provided";
      log.warn(errorMessage);
      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(HttpStatus.BAD_REQUEST, errorMessage, 
	  parsedRequest);
    }

    log.debug("uri '" + uri + "' is valid.");
  }

  private void validateArtifactExists(String collectionid, String artifactid, String errorMessage,
      String parsedRequest) throws IOException {
    if (!repo.artifactExists(collectionid, artifactid)) {
      log.warn(errorMessage);
      log.warn("Parsed request: " + parsedRequest);

      throw new LockssRestServiceException(HttpStatus.NOT_FOUND, errorMessage, 
	  parsedRequest);
    }

    log.debug("artifactid '" + artifactid + "' exists.");
  }

  @Override
  public Optional<ObjectMapper> getObjectMapper() {
    return Optional.ofNullable(objectMapper);
  }

  @Override
  public Optional<HttpServletRequest> getRequest() {
    return Optional.ofNullable(request);
  }

  /**
   * Provides the status object.
   * 
   * @return an ApiStatus with the status.
   */
  @Override
  public ApiStatus getApiStatus() {
    return new ApiStatus("swagger/swagger.yaml")
      .setReady(repo.isReady());
  }
}
