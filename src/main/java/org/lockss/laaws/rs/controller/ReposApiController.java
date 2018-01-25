package org.lockss.laaws.rs.controller;

import io.swagger.annotations.ApiParam;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.*;
import org.lockss.laaws.rs.api.ReposApi;
import org.lockss.laaws.rs.util.ArtifactFactory;
import org.lockss.laaws.rs.util.ArtifactUtil;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.model.ArtifactIndexData;
import org.lockss.laaws.rs.io.index.ArtifactPredicateBuilder;
import org.lockss.laaws.rs.io.storage.ArtifactStore;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
@Controller
public class ReposApiController implements ReposApi {
    private final static Log log = LogFactory.getLog(ReposApiController.class);

    @Autowired
    ArtifactStore artifactStore;

    @Autowired
    ArtifactIndex artifactIndex;

    // TODO: Figure out a better place to put this?
    public static final String APPLICATION_HTTP_RESPONSE_VALUE = "application/http;msgtype=response";
    public static final MediaType APPLICATION_HTTP_RESPONSE = MediaType.parseMediaType(APPLICATION_HTTP_RESPONSE_VALUE);

    /**
     * Implementation of GET on /repos: Returns a list of repository names
     *
     * This works by creating a Solr "facet" query, which generates a response which includes the counts for each value
     * for each field over the documents in the query result. The query is set to match all documents ('q=*:*'). We get
     * the facet result page for the "repository" field (using FacetPage#getFacetResultPage), then iterate through the
     * facet field's entries for value and value count, storing the former into an ArrayList to return.
     *
     * It is not clear how this scales to large Solr collections.
     */
    public ResponseEntity<List<String>> reposGet() {
        /*
        FacetQuery query = new SimpleFacetQuery(new Criteria(Criteria.WILDCARD).expression(Criteria.WILDCARD));
        query.setFacetOptions(new FacetOptions().addFacetOnField("repository"));

        // Not interested in the actual results
        query.setRows(0);

        // Submit and receive facet query results from Solr
        FacetPage<SolrArtifactIndexData> page = solrTemplate.queryForFacetPage(query, SolrArtifactIndexData.class);

        // Build a list of repository names
        List<String> repos = new ArrayList<String>();
        for (FacetFieldEntry ffe: page.getFacetResultPage("repository")) {
            //log.info(ffe.getValue() + ": " + ffe.getValueCount());
            repos.add(ffe.getValue());
        }
        */

        List<String> collectionIds = new ArrayList<>();
        artifactIndex.getCollectionIds().forEachRemaining(x -> collectionIds.add(x));
        return new ResponseEntity<>(collectionIds, HttpStatus.OK);
    }

    /**
     * Implementation of DELETE on /repos/{repository}/artifacts/{artifactid}: Deletes an artifact from the repository
     *
     * @param repository
     * @param artifactid
     * @return
     */
    public ResponseEntity<Void> reposArtifactsArtifactidDelete(
            @ApiParam(value = "Repository to add artifact into", required=true) @PathVariable("repository") String repository,
            @ApiParam(value = "Artifact ID", required=true) @PathVariable("artifactid") String artifactid) {

        // Check that the artifact exists
        if (!artifactIndex.artifactExists(artifactid))
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);

        // Remove the artifact from the artifact store and index
        artifactStore.deleteArtifact(artifactIndex.getArtifactIndexData(artifactid));
        artifactIndex.deleteArtifact(artifactid);

        return new ResponseEntity<>(HttpStatus.OK);
    }

    /**
     * Implementation of GET on /repos/{repository}/artifacts/{artifactid}: Returns an artifact from the repository
     *
     * @param repository
     * @param artifactId
     * @return
     * @throws IOException
     */
    public ResponseEntity<StreamingResponseBody> reposArtifactsArtifactidGet(
            @ApiParam(value = "Repository to add artifact into", required = true) @PathVariable("repository") String repository,
            @ApiParam(value = "Artifact ID", required = true) @PathVariable("artifactid") String artifactId
    ) throws IOException {

        // Make sure the artifact exists
        if (!artifactIndex.artifactExists(artifactId))
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);

        // Retrieve the Artifact from the artifact store
        Artifact artifact = artifactStore.getArtifact(artifactIndex.getArtifactIndexData(artifactId));

        log.info(String.format("Retrieving artifact: %s from collection %s", artifactId, repository));

        // Setup HTTP response headers
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.parseMediaType("application/http; msgtype=response"));
        // TODO: headers.setContentLength();

        return new ResponseEntity<>(
                outputStream -> {
                    try {
                        ArtifactUtil.writeHttpResponse(
                                ArtifactUtil.getHttpResponseFromArtifact(artifact),
                                outputStream
                        );
                    } catch (HttpException e) {
                        e.printStackTrace();
                    }
                },
                headers,
                HttpStatus.OK
        );
    }

    /**
     * Implementation of PUT on /repos/{repository}/artifacts/{artifactid}: Updates an artifact's properties
     *
     * Currently limited to updating an artifact's committed status.
     *
     * @param repository
     * @param artifactid
     * @param committed
     * @return
     */
    public ResponseEntity<String> reposArtifactsArtifactidPut(
            @ApiParam(value = "Repository to add artifact into",required=true ) @PathVariable("repository") String repository,
            @ApiParam(value = "Artifact ID",required=true ) @PathVariable("artifactid") String artifactid,
            @ApiParam(value = "New commit status of artifact") @RequestPart(value="committed", required=false) Boolean committed) {

        // Make sure that the artifact exists
        if (!artifactIndex.artifactExists(artifactid))
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);

        // Return bad request if new commit status has not been passed
        if (committed == null)
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);

        ArtifactIndexData indexData = artifactIndex.getArtifactIndexData(artifactid);

        log.info(String.format(
                "Updating commit status for %s (%s -> %s)",
                artifactid,
                indexData.getCommitted(),
                committed
        ));

        // Record the commit status in storage
        //artifactStore.updateArtifact(indexData, null);

        // Change the commit status in the index
        artifactIndex.commitArtifact(artifactid);

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
            @ApiParam(value = "Artifact ID") @RequestParam(value = "artifact", required = false) String artifact,
            @ApiParam(value = "Artifact AUID") @RequestParam(value = "auid", required = false) String auid,
            @ApiParam(value = "Artifact URI") @RequestParam(value = "uri", required = false) String uri,
            @ApiParam(value = "Artifact aspect") @RequestParam(value = "aspect", required = false) String aspect,
            @ApiParam(value = "Date and time associated with artifact's content") @RequestParam(value = "timestamp", required = false) Integer timestamp,
            @ApiParam(value = "Date and time of artifact acquisition into repository") @RequestParam(value = "acquired", required = false) Integer acquired,
            @ApiParam(value = "Artifact content digest") @RequestParam(value = "hash", required = false) String hash,
            @ApiParam(value = "Artifact committed status", defaultValue = "true") @RequestParam(value = "committed", required = false, defaultValue="true") Boolean committed,
            @ApiParam(value = "Include artifact aspects in results (default: false)", defaultValue = "false") @RequestParam(value = "includeAllAspects", required = false, defaultValue="false") Boolean includeAllAspects,
            @ApiParam(value = "Includes all versions if set (default: false)", defaultValue = "false") @RequestParam(value = "includeAllVersions", required = false, defaultValue="false") Boolean includeAllVersions,
            @ApiParam(value = "Maximum number of results to return (used for pagination)", defaultValue = "1000") @RequestParam(value = "limit", required = false, defaultValue="1000") Integer limit,
            @ApiParam(value = "Begin listing with given artifact (used for pagination)") @RequestParam(value = "next_artifact", required = false) String nextArtifact
    ) {

        ArtifactPredicateBuilder query = new ArtifactPredicateBuilder()
                .filterByCommitStatus(committed)
                .filterByCollection(repository)
                .filterByAuid(auid)
                .filterByURIPrefix(uri);


        Iterator<ArtifactIndexData> result = artifactIndex.query(query);

        List<String> artifacts = new ArrayList<>();
        result.forEachRemaining(x -> artifacts.add(x.getId()));
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
     public ResponseEntity<String> reposArtifactsPost(
            @ApiParam(value = "",required=true ) @PathVariable("repository") String repository,
            @ApiParam(value = "Archival Unit ID (AUID) of new artifact", required=true) @RequestPart(value="auid", required=true) String auid,
            @ApiParam(value = "URI represented by this artifact", required=true) @RequestPart(value="uri", required=true) String uri,
            @ApiParam(value = "Artifact version", required=true) @RequestPart(value="version", required=true) Integer version,
            @ApiParam(value = "Artifact") @RequestPart(value = "artifact", required=true) MultipartFile artifactPart,
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

        log.info(String.format("%s, %s, %s", repository, auid, uri));

        String artifactId = null;

        try {
            log.info(String.format("MultipartFile: Type: Artifact, Content-type: %s", artifactPart.getContentType()));

            // Only accept artifact encoded within an HTTP response
            if (!isHttpResponseType(MediaType.parseMediaType(artifactPart.getContentType()))) {

                log.error(String.format("Failed to add artifact; expected %s but got %s",
                        APPLICATION_HTTP_RESPONSE,
                        MediaType.parseMediaType(artifactPart.getContentType())));

                return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
            }

            Artifact artifact = ArtifactFactory.fromHttpResponseStream(artifactPart.getInputStream());
            artifactStore.addArtifact(artifact);

            // Index artifact into Solr
            ArtifactIndexData id = artifactIndex.indexArtifact(artifact);
            artifactId = id.getId();

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
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new ResponseEntity<>(artifactId, HttpStatus.OK);
    }

    private static Boolean isHttpResponseType(MediaType type) {
        return (APPLICATION_HTTP_RESPONSE.isCompatibleWith(type)
                && (type.getParameters().equals(APPLICATION_HTTP_RESPONSE.getParameters())));
    }

}
