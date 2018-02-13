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

package org.lockss.laaws.rs.io.index;

import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIndexData;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Interface of the artifact index.
 */
public interface ArtifactIndex {
    /**
     * Adds an artifact to the index.
     * 
     * @param artifact
     *          An Artifact with the artifact to be added to the index,.
     * @return an ArtifactIndexData with the artifact indexing data.
     */
    ArtifactIndexData indexArtifact(Artifact artifact);

    /**
     * Provides the index data of an artifact with a given text index
     * identifier.
     * 
     * @param indexDataId
     *          A String with the artifact index identifier.
     * @return an ArtifactIndexData with the artifact indexing data.
     */
    ArtifactIndexData getArtifactIndexData(String indexDataId);

    /**
     * Provides the index data of an artifact with a given index identifier
     * UUID.
     * 
     * @param indexDataId
     *          An UUID with the artifact index identifier.
     * @return an ArtifactIndexData with the artifact indexing data.
     */
    ArtifactIndexData getArtifactIndexData(UUID indexDataId);

    /**
     * Commits to the index an artifact with a given text index identifier.
     * 
     * @param indexDataId
     *          A String with the artifact index identifier.
     * @return an ArtifactIndexData with the committed artifact indexing data.
     */
    ArtifactIndexData commitArtifact(String indexDataId);

    /**
     * Commits to the index an artifact with a given index identifier UUID.
     * 
     * @param indexDataId
     *          An UUID with the artifact index identifier.
     * @return an ArtifactIndexData with the committed artifact indexing data.
     */
    ArtifactIndexData commitArtifact(UUID indexDataId);

    /**
     * Removes from the index an artifact with a given text index identifier.
     * 
     * @param indexDataId
     *          A String with the artifact index identifier.
     * @return <code>true</code> if the artifact was removed from in the index,
     * <code>false</code> otherwise.
     */
    boolean deleteArtifact(String indexDataId);

    /**
     * Removes from the index an artifact with a given index identifier UUID.
     * 
     * @param indexDataId
     *          A String with the artifact index identifier.
     * @return <code>true</code> if the artifact was removed from in the index,
     * <code>false</code> otherwise.
     */
    boolean deleteArtifact(UUID indexDataId);

    /**
     * Provides an indication of whether an artifact with a given text index
     * identifier exists in the index.
     * 
     * @param artifactId
     *          A String with the artifact identifier.
     * @return <code>true</code> if the artifact exists in the index,
     * <code>false</code> otherwise.
     */
    boolean artifactExists(String artifactId);

    /**
     * Provides the collection identifiers of the committed artifacts in the
     * index.
     * 
     * @return an {@code Iterator<String>} with the index committed artifacts
     * collection identifiers.
     */
    Iterator<String> getCollectionIds();

    /**
     * Provides the committed artifacts in a collection grouped by the
     * identifier of the Archival Unit to which they belong.
     * 
     * @param collection
     *          A String with the collection identifier.
     * @return a {@code Map<String, List<ArtifactIndexData>>} with the committed
     *         artifacts in the collection grouped by the identifier of the
     *         Archival Unit to which they belong.
     */
    Map<String, List<ArtifactIndexData>> getAus(String collection);

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit.
     * 
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @return an {@code Iterator<ArtifactIndexData>} with the committed
     *         artifacts in the collection that belong to the Archival Unit.
     */
    Iterator<ArtifactIndexData> getArtifactsInAU(String collection, String auid);

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit and that contain a URL with a given prefix.
     * 
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param prefix
     *          A String with the URL prefix.
     * @return an {@code Iterator<ArtifactIndexData>} with the committed
     *         artifacts in the collection that belong to the Archival Unit and
     *         that contain a URL with the given prefix.
     */
    Iterator<ArtifactIndexData> getArtifactsInAUWithURL(String collection, String auid, String prefix);

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit and that contain an exact match of a URL.
     * 
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param url
     *          A String with the URL to be matched.
     * @return an {@code Iterator<ArtifactIndexData>} with the committed
     *         artifacts in the collection that belong to the Archival Unit and
     *         that contain an exact match of a URL.
     */
    Iterator<ArtifactIndexData> getArtifactsInAUWithURLMatch(String collection,
	String auid, String url);

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit and that contain a URL with a given prefix and that match a
     * given version.
     * 
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param prefix
     *          A String with the URL prefix.
     * @param version
     *          A String with the version.
     * @return an {@code Iterator<ArtifactIndexData>} with the committed
     *         artifacts in the collection that belong to the Archival Unit and
     *         that contain a URL with the given prefix and that match the given
     *         version.
     */
    Iterator<ArtifactIndexData> getArtifactsInAUWithURL(String collection,
	String auid, String prefix, String version);

    /**
     * Provides the committed artifacts in a collection that belong to an
     * Archival Unit and that contain an exact match of a URL and that match a
     * given version.
     * 
     * @param collection
     *          A String with the collection identifier.
     * @param auid
     *          A String with the Archival Unit identifier.
     * @param url
     *          A String with the URL to be matched.
     * @param version
     *          A String with the version.
     * @return an {@code Iterator<ArtifactIndexData>} with the committed
     *         artifacts in the collection that belong to the Archival Unit and
     *         that contain an exact match of a URL and that match the given
     *         version.
     */
    Iterator<ArtifactIndexData> getArtifactsInAUWithURLMatch(String collection,
	String auid, String url, String version);

    /**
     * Provides the artifacts in the index that result from a given query.
     * 
     * @param query
     *          An {@code Iterator<ArtifactIndexData>} with the query.
     * @return an {@code Iterator<ArtifactIndexData>} with the artifacts
     *         resulting from the query.
     */
    Iterator<ArtifactIndexData> query(ArtifactPredicateBuilder query);
}
