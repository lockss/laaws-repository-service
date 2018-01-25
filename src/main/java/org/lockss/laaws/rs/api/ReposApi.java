/*
 * Copyright (c) 2017, Board of Trustees of Leland Stanford Jr. University,
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

import io.swagger.annotations.*;
import org.lockss.laaws.rs.io.index.solr.SolrArtifactIndexData;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.util.List;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.SpringCodegen", date = "2017-06-09T15:47:21.006-07:00")

@Api(value = "repos", description = "the repos API")
public interface ReposApi {

    @ApiOperation(value = "Get the list of repos", notes = "", response = String.class, responseContainer = "List", tags={  })
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "Status 200", response = String.class, responseContainer = "List") })

    @RequestMapping(value = "/repos",
        produces = { "application/json" },
        //consumes = { "application/json" },
        method = RequestMethod.GET)
    ResponseEntity<List<String>> reposGet();


    @ApiOperation(value = "Delete artifact", notes = "", response = Void.class, tags={  })
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "Successfully removed artifact", response = Void.class),
        @ApiResponse(code = 401, message = "Unauthorized client", response = Void.class),
        @ApiResponse(code = 403, message = "Client not authorized to delete artifact", response = Void.class),
        @ApiResponse(code = 404, message = "Artifact not found", response = Void.class),
        @ApiResponse(code = 409, message = "Cannot delete committed artifact", response = Void.class) })

    @RequestMapping(value = "/repos/{repository}/artifacts/{artifactid}",
        produces = { "application/json" },
        //consumes = { "application/json" },
        method = RequestMethod.DELETE)
    ResponseEntity<Void> reposArtifactsArtifactidDelete(@ApiParam(value = "Repository to add artifact into",required=true ) @PathVariable("repository") String repository,@ApiParam(value = "Artifact ID",required=true ) @PathVariable("artifactid") String artifactid);


    @ApiOperation(value = "Get artifact content and metadata", notes = "", response = Void.class, tags={  })
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Void.class),
        @ApiResponse(code = 401, message = "Unauthorized client", response = Void.class),
        @ApiResponse(code = 403, message = "Client not authorized to retrieve artifact", response = Void.class),
        @ApiResponse(code = 404, message = "Artifact not found", response = Void.class),
        @ApiResponse(code = 502, message = "Could not read from external resource", response = Void.class) })

    @RequestMapping(value = "/repos/{repository}/artifacts/{artifactid}",
        produces = { "multipart/form-data" },
        //consumes = { "application/json" },
        method = RequestMethod.GET)
    ResponseEntity<StreamingResponseBody> reposArtifactsArtifactidGet(@ApiParam(value = "Repository to add artifact into",required=true ) @PathVariable("repository") String repository,@ApiParam(value = "ArtifactInfo ID",required=true ) @PathVariable("artifactid") String artifactid) throws IOException;


    @ApiOperation(value = "Update artifact metadata", notes = "", response = SolrArtifactIndexData.class, tags={  })
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "Artifact updated", response = SolrArtifactIndexData.class),
        @ApiResponse(code = 400, message = "Invalid input", response = Void.class),
        @ApiResponse(code = 401, message = "Unauthorized client", response = Void.class),
        @ApiResponse(code = 403, message = "Client not authorized to update artifact", response = Void.class),
        @ApiResponse(code = 404, message = "Artifact not found", response = Void.class) })

    @RequestMapping(value = "/repos/{repository}/artifacts/{artifactid}",
        produces = { "application/json" },
        consumes = { "multipart/form-data" },
        method = RequestMethod.PUT)
    ResponseEntity<String> reposArtifactsArtifactidPut(@ApiParam(value = "Repository to add artifact into",required=true ) @PathVariable("repository") String repository, @ApiParam(value = "Artifact ID",required=true ) @PathVariable("artifactid") String artifactid, @ApiParam(value = "New commit status of artifact") @RequestPart(value="committed", required=false)  Boolean committed);


    @ApiOperation(value = "Query repository for artifacts", notes = "", response = Object.class, tags={  })
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "OK", response = Object.class),
            @ApiResponse(code = 400, message = "Invalid input", response = Object.class),
            @ApiResponse(code = 401, message = "Unauthorized client", response = Object.class),
            @ApiResponse(code = 403, message = "Client not allowed to query repository", response = Object.class),
            @ApiResponse(code = 404, message = "Repository not found", response = Object.class),
            @ApiResponse(code = 502, message = "Error experienced with internal database", response = Object.class)
    })
    @RequestMapping(
            value = "/repos/{repository}/artifacts",
            produces = { "application/json" },
            //consumes = { "application/json" },
            method = RequestMethod.GET
    )
    ResponseEntity<List<String>> reposArtifactsGet(
            @ApiParam(value = "",required=true ) @PathVariable("repository") String repository,
            @ApiParam(value = "Artifact ID") @RequestParam(value = "artifact", required = false) String artifact,
            @ApiParam(value = "Artifact AUID") @RequestParam(value = "auid", required = false) String auid,
            @ApiParam(value = "Artifact URI") @RequestParam(value = "uri", required = false) String uri,
            @ApiParam(value = "Artifact URI aspect") @RequestParam(value = "aspect", required = false) String aspect,
            @ApiParam(value = "Date and time associated with artifact's content") @RequestParam(value = "timestamp", required = false) Integer timestamp,
            @ApiParam(value = "Date and time of artifact acquistion into repository") @RequestParam(value = "acquired", required = false) Integer acquired,
            @ApiParam(value = "Artifact content hash") @RequestParam(value = "hash", required = false) String hash,
            @ApiParam(value = "Artifact committed status", defaultValue = "true") @RequestParam(value = "committed", required = false, defaultValue="true") Boolean committed,
            @ApiParam(value = "Query results will include all aspects if set to true (default: false)", defaultValue = "false") @RequestParam(value = "includeAllAspects", required = false, defaultValue="false") Boolean includeAllAspects,
            @ApiParam(value = "Includes all versions if set (default: false)", defaultValue = "false") @RequestParam(value = "includeAllVersions", required = false, defaultValue="false") Boolean includeAllVersions,
            @ApiParam(value = "Maximum number of results to return (used for pagination)", defaultValue = "100") @RequestParam(value = "limit", required = false, defaultValue="100") Integer limit,
            @ApiParam(value = "Begin listing with given artifact (used for pagination)") @RequestParam(value = "nextArtifact", required = false) String nextArtifact
    );


    @ApiOperation(value = "Create an artifact", notes = "", response = SolrArtifactIndexData.class, tags={  })
    @ApiResponses(value = {
        @ApiResponse(code = 201, message = "Artifact created", response = SolrArtifactIndexData.class),
        @ApiResponse(code = 302, message = "Duplicate content; artifact not created", response = Void.class),
        @ApiResponse(code = 400, message = "Invalid input", response = Void.class),
        @ApiResponse(code = 401, message = "Unauthorized client", response = Void.class),
        @ApiResponse(code = 403, message = "Client not authorized to create artifacts", response = Void.class),
        @ApiResponse(code = 502, message = "Internal error creating artifact", response = Void.class) })

    @RequestMapping(
        value = "/repos/{repository}/artifacts",
        produces = { "application/json" },
        consumes = { "multipart/form-data" },
        method = RequestMethod.POST
    )
    ResponseEntity<String> reposArtifactsPost(
            @ApiParam(value = "",required=true ) @PathVariable("repository") String repository,
            @ApiParam(value = "Archival Unit ID (AUID) of new artifact", required=true) @RequestPart(value="auid", required=true) String auid,
            @ApiParam(value = "URI represented by this artifact", required=true) @RequestPart(value="uri", required=true) String uri,
            @ApiParam(value = "Artifact version", required=true) @RequestPart(value="version", required=true) Integer version,
            @ApiParam(value = "Artifact") @RequestPart(value = "artifact", required=true) MultipartFile artifactPart,
            @ApiParam(value = "Aspects") @RequestPart("aspects") MultipartFile... aspectParts
    );

}
