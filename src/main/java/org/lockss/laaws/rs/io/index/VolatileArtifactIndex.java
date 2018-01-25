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

package org.lockss.laaws.rs.io.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIndexData;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VolatileArtifactIndex implements ArtifactIndex {
    private final static Log log = LogFactory.getLog(VolatileArtifactIndex.class);

    // Map from artifact ID to ArtifactIndexData
    private Map<String, ArtifactIndexData> index = new LinkedHashMap<>();

    @Override
    public ArtifactIndexData indexArtifact(Artifact artifact) {
        // New UUID for the object representing this artifact in the index (TODO: Resolve collisions)
        UUID indexDataId = UUID.randomUUID();

        ArtifactIdentifier artifactIdentifier = artifact.getIdentifier();

        ArtifactIndexData indexData = new ArtifactIndexData(
                indexDataId.toString(),
                artifactIdentifier.getCollection(),
                artifactIdentifier.getAuid(),
                artifactIdentifier.getUri(),
                artifactIdentifier.getVersion(),
                false
        );

        index.put(indexDataId.toString(), indexData);

        return indexData;
    }

    @Override
    public ArtifactIndexData getArtifactIndexData(String indexDataId) {
        return index.get(indexDataId);
    }

    @Override
    public ArtifactIndexData getArtifactIndexData(UUID indexDataId) {
        return getArtifactIndexData(indexDataId.toString());
    }

    @Override
    public ArtifactIndexData commitArtifact(String indexDataId) {
        ArtifactIndexData indexedData = index.get(indexDataId);
        indexedData.setCommitted(true);

        return indexedData;
    }

    @Override
    public ArtifactIndexData commitArtifact(UUID indexDataId) {
        return commitArtifact(indexDataId.toString());
    }

    @Override
    public void deleteArtifact(String indexDataId) {
        index.remove(indexDataId);
    }

    @Override
    public void deleteArtifact(UUID indexDataId) {
        deleteArtifact(indexDataId.toString());
    }

    @Override
    public boolean artifactExists(String artifactId) {
        return index.containsKey(artifactId);
    }

    @Override
    public Iterator<String> getCollectionIds() {
        Stream<ArtifactIndexData> artifactStream = index.values().stream();
        Stream<ArtifactIndexData> committedArtifacts = artifactStream.filter(x -> x.getCommitted());
        Map<String, List<ArtifactIndexData>> collections = committedArtifacts.collect(Collectors.groupingBy(ArtifactIndexData::getCollection));

        return collections.keySet().iterator();
    }

    @Override
    public Map<String, List<ArtifactIndexData>> getAus(String collection) {
        return getCommittedArtifacts(collection).collect(Collectors.groupingBy(ArtifactIndexData::getAuid));
    }

    @Override
    public Iterator<ArtifactIndexData> getArtifactsInAU(String collection, String auid) {
        ArtifactPredicateBuilder query = new ArtifactPredicateBuilder();
        query.filterByCommitStatus(true);
        query.filterByCollection(collection);
        query.filterByAuid(auid);

        return index.values().stream().filter(query.build()).iterator();
    }

    @Override
    public Iterator<ArtifactIndexData> getArtifactsinAUWithURL(String collection, String auid, String prefix) {
        ArtifactPredicateBuilder query = new ArtifactPredicateBuilder();
        query.filterByCommitStatus(true);
        query.filterByCollection(collection);
        query.filterByAuid(auid);
        query.filterByURIPrefix(prefix);

        return index.values().stream().filter(query.build()).iterator();
    }

    @Override
    public Iterator<ArtifactIndexData> query(ArtifactPredicateBuilder query) {
        return index.values().stream().filter(query.build()).iterator();
    }

    public Stream<ArtifactIndexData> getCommittedArtifacts(String collection) {
        ArtifactPredicateBuilder query = new ArtifactPredicateBuilder();
        query.filterByCommitStatus(true);
        query.filterByCollection(collection);

        return index.values().stream().filter(query.build());
    }
}
