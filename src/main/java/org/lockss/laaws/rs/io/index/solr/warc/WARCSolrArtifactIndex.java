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

package org.lockss.laaws.rs.io.index.solr.warc;

import org.apache.http.Header;
import org.codehaus.jackson.annotate.JsonProperty;
import org.lockss.laaws.rs.io.index.solr.SolrArtifactIndexData;
import org.springframework.data.solr.core.mapping.Indexed;
import org.springframework.data.solr.core.mapping.SolrDocument;

import java.io.*;


/**
 * Models the Solr document to hold WARC specific indexing; inspired by webarchive-common's WarcRecordInfo
 */
// TODO: Make the Solr core name configurable
//@SolrDocument(solrCoreName = "WARCArtifactMetadata")
@SolrDocument(solrCoreName = "test-core")
public class WARCSolrArtifactIndex extends SolrArtifactIndexData {
    // WARC record addressing (WARC file path, and WARC record offset within WARC file)
    @Indexed
    @JsonProperty("WARCFilePath")
    private String warcFilePath;

    @JsonProperty("WARCFileOffset")
    private long warcFileOffset;

    // Mandatory WARC named fields (see "WARC File Format" ISO 28500)
    @JsonProperty("WARCRecordId")
    private String recordId;

    @JsonProperty("WARCRecordType")
    private String recordType;

    @JsonProperty("WARCRecordDate")
    private String recordDate;

    @JsonProperty("WARCRecordContentLength")
    private Integer recordContentLength;

    public String getWARCFilePath() {
        return warcFilePath;
    }

    public void setWARCFilePath(String warcFilePath) {
        this.warcFilePath = warcFilePath;
    }

    public long getWARCFileOffset() {
        return warcFileOffset;
    }

    public void setWARCFileOffset(long warcFileOffset) {
        this.warcFileOffset = warcFileOffset;
    }

    public Header[] getWARCHeaders() throws IOException {
        return null;
    }

    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    public String getRecordType() {
        return recordType;
    }

    public void setRecordType(String recordType) {
        this.recordType = recordType;
    }

    public String getRecordDate() {
        return recordDate;
    }

    public void setRecordDate(String recordDate) {
        this.recordDate = recordDate;
    }

    public Integer getRecordContentLength() {
        return recordContentLength;
    }

    public void setRecordContentLength(Integer recordContentLength) {
        this.recordContentLength = recordContentLength;
    }
}

