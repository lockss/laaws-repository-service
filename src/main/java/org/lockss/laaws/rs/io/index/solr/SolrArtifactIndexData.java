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

package org.lockss.laaws.rs.io.index.solr;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import org.apache.http.*;
import org.apache.solr.client.solrj.beans.Field;
import org.springframework.data.annotation.Id;
import org.springframework.data.solr.core.mapping.Indexed;

public class SolrArtifactIndexData {
    // TODO: Make a builder class for this

    @Id
    @Field
    @JsonProperty("ArtifactId")
    private String id = null;

    @Indexed
    @JsonProperty("ArtifactCollection")
    private String collection = null;

    @Indexed
    @JsonProperty("ArtifactAuId")
    private String auid = null;

    @Indexed
    @JsonProperty("ArtifactURI")
    private String uri = null;

    @JsonProperty("ArtifactContentType")
    private String contentType = null;

    @JsonProperty("ArtifactContentLength")
    private Integer contentLength = null;

    @JsonProperty("ArtifactDate")
    private String date = null;

    @JsonProperty("ArtifactModified")
    private String lastModified = null;

    @JsonProperty("ArtifactContentHash")
    private String contentHash = null;

    @JsonProperty("ArtifactCommitted")
    private boolean committed;

    public Header[] getHeaders() {
        return null;
    }

    /**
     * Get id
     *
     * @return id
     **/
    @ApiModelProperty(value = "")


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public SolrArtifactIndexData id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Get collection
     *
     * @return collection
     **/
    @ApiModelProperty(value = "")


    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public SolrArtifactIndexData collection(String collection) {
        this.collection = collection;
        return this;
    }

    /**
     * Get auid
     *
     * @return auid
     **/
    @ApiModelProperty(value = "")


    public String getAuid() {
        return auid;
    }

    public void setAuid(String auid) {
        this.auid = auid;
    }

    public SolrArtifactIndexData auid(String auid) {
        this.auid = auid;
        return this;
    }

    /**
     * Get uri
     *
     * @return uri
     **/
    @ApiModelProperty(value = "")


    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public SolrArtifactIndexData uri(String uri) {
        this.uri = uri;
        return this;
    }

    /**
     * Get committed
     *
     * @return committed
     **/
    @ApiModelProperty(value = "")


    public boolean getCommitted() {
        return committed;
    }

    public void setCommitted(boolean committed) {
        this.committed = committed;
    }

    public SolrArtifactIndexData committed(boolean committed) {
        this.committed = committed;
        return this;
    }

    /**
     * Get contentType
     *
     * @return contentType
     **/
    @ApiModelProperty(value = "")


    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public SolrArtifactIndexData contentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    /**
     * Get contentLength
     *
     * @return contentLength
     **/
    @ApiModelProperty(value = "")


    public Integer getContentLength() {
        return contentLength;
    }

    public void setContentLength(Integer contentLength) {
        this.contentLength = contentLength;
    }

    public SolrArtifactIndexData contentLength(Integer contentLength) {
        this.contentLength = contentLength;
        return this;
    }

    /**
     * Get date
     *
     * @return date
     **/
    @ApiModelProperty(value = "")


    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public SolrArtifactIndexData date(String date) {
        this.date = date;
        return this;
    }

    /**
     * Get lastModified
     *
     * @return lastModified
     **/
    @ApiModelProperty(value = "")


    public String getLastModified() {
        return lastModified;
    }

    public void setLastModified(String lastModified) {
        this.lastModified = lastModified;
    }

    public SolrArtifactIndexData contentModified(String contentModified) {
        this.lastModified = contentModified;
        return this;
    }

    /**
     * Get contentHash
     *
     * @return contentHash
     **/
    @ApiModelProperty(value = "")


    public String getContentHash() {
        return contentHash;
    }

    public void setContentHash(String contentHash) {
        this.contentHash = contentHash;
    }

    public SolrArtifactIndexData contentHash(String contentHash) {
        this.contentHash = contentHash;
        return this;
    }

    @Override
    public boolean equals(java.lang.Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return Objects.equals(this.id, ((SolrArtifactIndexData)o).getId());
                /*
                Objects.equals(this.auid, artifact.getAuid()) &&
                Objects.equals(this.uri, artifact.getUri()) &&
                Objects.equals(this.aspect, artifact.getAspect) &&
                Objects.equals(this.acquired, artifact.getAcquired) &&
                Objects.equals(this.contentHash, artifact.contentHash) &&
                Objects.equals(this.metadataHash, artifact.metadataHash) &&
                Objects.equals(this.contentLength, artifact.contentLength) &&
                Objects.equals(this.contentDatetime, artifact.contentDatetime);
                */
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, collection, auid, uri, committed, contentType, contentLength, date, lastModified, contentHash);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class SolrArtifactIndexData {\n");

        sb.append("    id: ").append(toIndentedString(id)).append("\n");
        sb.append("    collection: ").append(toIndentedString(collection)).append("\n");
        sb.append("    auid: ").append(toIndentedString(auid)).append("\n");
        sb.append("    uri: ").append(toIndentedString(uri)).append("\n");
        sb.append("    committed: ").append(toIndentedString(committed)).append("\n");
        sb.append("    contentType: ").append(toIndentedString(contentType)).append("\n");
        sb.append("    contentLength: ").append(toIndentedString(contentLength)).append("\n");
        sb.append("    contentDate: ").append(toIndentedString(date)).append("\n");
        sb.append("    lastModified: ").append(toIndentedString(lastModified)).append("\n");
        sb.append("    contentHash: ").append(toIndentedString(contentHash)).append("\n");
        sb.append("}");
        return sb.toString();
    }

    /**
     * Convert the given object to string with each line indented by 4 spaces
     * (except the first line).
     */
    private String toIndentedString(java.lang.Object o) {
        if (o == null) {
            return "null";
        }
        return o.toString().replace("\n", "\n    ");
    }

}

