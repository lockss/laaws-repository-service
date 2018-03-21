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

package org.lockss.laaws.rs.controller;

import io.swagger.annotations.ApiParam;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.*;
import org.lockss.laaws.rs.api.ReposApi;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.util.ArtifactConstants;
import org.lockss.laaws.rs.util.ArtifactDataFactory;
import org.lockss.laaws.rs.util.ArtifactDataUtil;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.io.index.ArtifactPredicateBuilder;
import org.lockss.laaws.rs.io.storage.ArtifactDataStore;
import org.lockss.laaws.rs.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Spring controller for the REST presentation of the LOCKSS Repository service.
 */
@Controller
public class ReposApiController implements ReposApi {
    private final static Log log = LogFactory.getLog(ReposApiController.class);
    public static final String APPLICATION_HTTP_RESPONSE_VALUE = "application/http;msgtype=response";
    public static final MediaType APPLICATION_HTTP_RESPONSE = MediaType.parseMediaType(APPLICATION_HTTP_RESPONSE_VALUE);

    @Autowired
    LockssRepository repo;

    /**
     * GET /repos: Returns a list of collection names managed by this repository.
     *
     * @return List of collection names.
     */
    public ResponseEntity<List<String>> reposGet() {
        List<String> collectionIds = new ArrayList<>();
        try {
            repo.getCollectionIds().forEach(x -> collectionIds.add(x));
        } catch (IOException e) {
            log.error("IOException was caught trying to enumerate collection IDs");
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(collectionIds, HttpStatus.OK);
    }

    /**
     * DELETE /repos/{collection}/artifacts/{artifactId}: Deletes an artifact from a collection managed by this repository.
     *
     * @param repository
     * @param artifactid
     * @return
     */
    public ResponseEntity<Void> reposArtifactsArtifactidDelete(
            @ApiParam(value = "Repository to add artifact into", required=true) @PathVariable("repository") String repository,
            @ApiParam(value = "ArtifactData ID", required=true) @PathVariable("artifactid") String artifactid
    ) {
        try {
            // Check that the artifact exists
            if (!repo.artifactExists(artifactid))
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);

            // Remove the artifact from the artifact store and index
            repo.deleteArtifact(repository, artifactid);
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
     * GET /repos/{collection}/artifacts/{artifactId}: Retrieves an artifact from the repository.
     *
     * @param repository
     * @param artifactId
     * @return
     */
    public ResponseEntity<StreamingResponseBody> reposArtifactsArtifactidGet(
            @ApiParam(value = "Repository to add artifact into", required = true) @PathVariable("repository") String repository,
            @ApiParam(value = "ArtifactData ID", required = true) @PathVariable("artifactid") String artifactId
    ) {
        log.info(String.format("Retrieving artifact: %s from collection %s", artifactId, repository));

        try {
            // Make sure the artifact exists
            if (!repo.artifactExists(artifactId))
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);

            // Retrieve the ArtifactData from the artifact store
            ArtifactData artifactData = repo.getArtifactData(repository, artifactId);

            // Setup HTTP response headers
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.parseMediaType("application/http; msgtype=response"));
            // TODO: headers.setContentLength();

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
                    artifactId,
                    e
            ));

            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Implementation of PUT on /repos/{repository}/artifacts/{artifactId}: Updates an artifact's properties
     *
     * Currently limited to updating an artifact's committed status.
     *
     * @param repository
     * @param artifactId
     * @param committed
     * @return
     */
    public ResponseEntity<String> reposArtifactsArtifactidPut(
            @ApiParam(value = "Repository to add artifact into",required=true ) @PathVariable("repository") String repository,
            @ApiParam(value = "ArtifactData ID",required=true ) @PathVariable("artifactid") String artifactId,
            @ApiParam(value = "New commit status of artifact") @RequestPart(value="committed", required=false) Boolean committed
    ) {
        // Return bad request if new commit status has not been passed
        if (committed == null)
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);

        try {
            // Make sure that the artifact exists
            if (!repo.artifactExists(artifactId))
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);

            log.info(String.format(
                    "Updating commit status for %s (%s -> %s)",
                    artifactId,
                    repo.isArtifactCommitted(artifactId),
                    committed
            ));

            // Record the commit status in storage
            repo.commitArtifact(repository, artifactId);
        } catch (IOException e) {
            log.error(String.format(
                    "IOException occurred while attempting to update artifact metadata (artifactId: %s): %s",
                    artifactId,
                    e
            ));

            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(HttpStatus.OK);
    }

    /**
     * Implementation of GET on /repos/{repository}/artifacts: Queries the repository for artifacts
     *
     * @param repository
     * @param artifact
     * @param auid
     * @param uri
     * @param aspect
     * @param timestamp
     * @param acquired
     * @param hash
     * @param committed
     * @param includeAllAspects
     * @param includeAllVersions
     * @param limit
     * @param nextArtifact
     * @return
     */
    public ResponseEntity<List<String>> reposArtifactsGet(
            @ApiParam(value = "Collection ID",required=true ) @PathVariable("repository") String repository,
            @ApiParam(value = "ArtifactData ID") @RequestParam(value = "artifact", required = false) String artifact,
            @ApiParam(value = "ArtifactData AUID") @RequestParam(value = "auid", required = false) String auid,
            @ApiParam(value = "ArtifactData URI") @RequestParam(value = "uri", required = false) String uri,
            @ApiParam(value = "ArtifactData aspect") @RequestParam(value = "aspect", required = false) String aspect,
            @ApiParam(value = "Date and time associated with artifact's content") @RequestParam(value = "timestamp", required = false) Integer timestamp,
            @ApiParam(value = "Date and time of artifact acquisition into repository") @RequestParam(value = "acquired", required = false) Integer acquired,
            @ApiParam(value = "ArtifactData content digest") @RequestParam(value = "hash", required = false) String hash,
            @ApiParam(value = "ArtifactData committed status", defaultValue = "true") @RequestParam(value = "committed", required = false, defaultValue="true") Boolean committed,
            @ApiParam(value = "Include artifact aspects in results (default: false)", defaultValue = "false") @RequestParam(value = "includeAllAspects", required = false, defaultValue="false") Boolean includeAllAspects,
            @ApiParam(value = "Includes all versions if set (default: false)", defaultValue = "false") @RequestParam(value = "includeAllVersions", required = false, defaultValue="false") Boolean includeAllVersions,
            @ApiParam(value = "Maximum number of results to return (used for pagination)", defaultValue = "1000") @RequestParam(value = "limit", required = false, defaultValue="1000") Integer limit,
            @ApiParam(value = "Begin listing with given artifact (used for pagination)") @RequestParam(value = "next_artifact", required = false) String nextArtifact
    ) {

//        ArtifactPredicateBuilder query = new ArtifactPredicateBuilder()
//                .filterByCommitStatus(committed)
//                .filterByCollection(repository)
//                .filterByAuid(auid)
//                .filterByURIPrefix(uri);


//        Iterator<Artifact> result = repo.queryArtifacts(query);

//        List<String> artifacts = new ArrayList<>();
//        result.forEachRemaining(x -> artifacts.add(x.getId()));
//        return new ResponseEntity<>(artifacts, HttpStatus.OK);

        List<String> artifacts = new ArrayList<String>();
        artifacts.add("ok");
        return new ResponseEntity<>(artifacts, HttpStatus.OK);

        /*
        // Begin with a default query that matches everything
        Query query = new SimpleQuery(new Criteria(Criteria.WILDCARD).expression(Criteria.WILDCARD));

        // Criteria on committed status: By default, return only artifacts that have been committed to the repository
        query.addCriteria(new Criteria("committed").is(committed));

        // Set maximum rows per page of results
        query.setRows(limit);

        // Set optional criteria
        if (artifact != null) query.addCriteria(new Criteria("id").is(artifact));
        if (auid != null) query.addCriteria(new Criteria("auid").is(auid));
        if (uri != null) query.addCriteria(new Criteria("uri").startsWith(uri));
        if (aspect != null) query.addCriteria(new Criteria("aspect").is(aspect));
        if (timestamp != null) query.addCriteria(new Criteria("timestamp").greaterThanEqual(timestamp));
        if (acquired != null) query.addCriteria(new Criteria("acquired").greaterThanEqual(acquired));
        if (hash != null) query.addCriteria(new Criteria("content_hash").is(hash));

        // TODO: Implement paging
        // query.setPageRequest(page.nextPageable());
        // query.setPageRequest();

        Page<SolrArtifactIndexData> page = solrTemplate.query(query, SolrArtifactIndexData.class);

        // TODO: Iterate and page through the Solr query result pages until we have fulfilled the our page request

        // Get the first and last element of
        page.getContent().get(0); // First element
        page.getContent().get(page.getNumberOfElements()); // Last element

        // Return "no content" if the query came back with no results
        //if (page.getTotalElements() == 0) {
        //    return new ResponseEntity<List<SolrArtifactIndexData>>(HttpStatus.NO_CONTENT);
        //}

        // Do paging and HATEOAS type stuff here:
        MultiValueMap<String, String> headers = new LinkedMultiValueMap<String, String>();

        if (page.hasNext()) {
            headers.add("X-LOCKSS-next", page.nextPageable().toString());
        }

        // HATEOAS stuff
        //headers.add("X-LOCKSS-next", "http://localhost:8080/repos/" + repository + "/");
        //headers.add("X-LOCKSS-prev", "http://localhost:8080/repos/" + repository + "/");
        //headers.add("X-LOCKSS-total", String.valueOf(page.getTotalElements()));
        //headers.add("X-LOCKSS-count", String.valueOf(page.getNumberOfElements()));

        // Build an ArtifactPage to return results of repository query
        ArtifactPage ap = new ArtifactPage();
        ap.setNext("http://localhost/next");
        ap.setPrev("http://localhost/next");
        ap.setResults((int)page.getTotalElements());
        ap.setPage(page.getNumberOfElements());
        //ap.setItems(page.getContent());

        return new ResponseEntity<ArtifactPage>(ap, headers, HttpStatus.OK);
        */

    }

    /**
     * Implementation of POST on /repos/{repository}/artifacts: Adds artifacts to the repository
     *
     * @param repository
     * @param auid
     * @param uri
     * @return
     */
     public ResponseEntity<Artifact> reposArtifactsPost(
            @ApiParam(value = "",required=true ) @PathVariable("repository") String repository,
            @ApiParam(value = "Archival Unit ID (AUID) of new artifact", required=true) @RequestPart(value="auid", required=true) String auid,
            @ApiParam(value = "URI represented by this artifact", required=true) @RequestPart(value="uri", required=true) String uri,
            @ApiParam(value = "ArtifactData version", required=true) @RequestPart(value="version", required=true) Integer version,
            @ApiParam(value = "ArtifactData") @RequestPart(value = "artifact", required=true) MultipartFile artifactPart,
            @ApiParam(value = "Aspects") @RequestPart("aspects") MultipartFile... aspectParts
     ) {

        // Create a new SolrArtifactIndexData and set some of its properties
        /*
        SolrArtifactIndexData artifactMetadata = new SolrArtifactIndexData();
        artifactMetadata.setCollection(repository);
        artifactMetadata.setAuid(auid);
        artifactMetadata.setUri(uri);
        //artifactMetadata.setVersion(version);
        artifactMetadata.setCommitted(false);
        */

        log.info(String.format("Adding artifact %s, %s, %s, %d", repository, auid, uri, version));

        try {
            log.info(String.format("MultipartFile: Type: ArtifactData, Content-type: %s", artifactPart.getContentType()));

            // Only accept artifact encoded within an HTTP response
            if (!isHttpResponseType(MediaType.parseMediaType(artifactPart.getContentType()))) {

                log.error(String.format("Failed to add artifact; expected %s but got %s",
                        APPLICATION_HTTP_RESPONSE,
                        MediaType.parseMediaType(artifactPart.getContentType())));

                return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
            }

            // Inject version header
            HttpHeaders headers = new HttpHeaders();
            headers.set(ArtifactConstants.ARTIFACTID_VERSION_KEY, String.valueOf(version));

            ArtifactData artifactData = ArtifactDataFactory.fromHttpResponseStream(headers, artifactPart.getInputStream());
            Artifact artifact = repo.addArtifact(artifactData);

            log.info(String.format("Wrote artifact to %s", artifactData.getStorageUrl()));

            // Index artifact into Solr
//            Artifact id = repo.indexArtifact(artifact);
//            artifactId = id.getId();

            // TODO: Process artifact's aspects
            for (MultipartFile aspectPart : aspectParts) {
                log.warn(String.format("Ignoring MultipartFile: Type: Aspect, Content-type: %s", aspectPart.getContentType()));
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
            }

            return new ResponseEntity<>(artifact, HttpStatus.OK);
        } catch (IOException e) {
            log.error("Caught IOException while attempting to add an artifact to the repository");
        }

        return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    private static Boolean isHttpResponseType(MediaType type) {
        return (APPLICATION_HTTP_RESPONSE.isCompatibleWith(type)
                && (type.getParameters().equals(APPLICATION_HTTP_RESPONSE.getParameters())));
    }

}
