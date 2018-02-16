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

package org.lockss.laaws.rs.io.storage.mock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.archive.io.warc.WARCRecord;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.ArtifactFactory;
import org.lockss.laaws.rs.io.storage.WarcArtifactStore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class VolatileWarcArtifactStore extends WarcArtifactStore {
    private final static Log log = LogFactory.getLog(VolatileWarcArtifactStore.class);
    private Map<String, Map<String, Map<String, byte[]>>> repository;

    public VolatileWarcArtifactStore() {
        this.repository = new HashMap<>();
    }

    @Override
    public Artifact addArtifact(Artifact artifact) throws IOException {
        // Get artifact identifier
        ArtifactIdentifier artifactId = artifact.getIdentifier();

        // Get the collection
        Map<String, Map<String, byte[]>> collection = repository.getOrDefault(artifactId.getCollection(), new HashMap<>());

        // Get the AU
        Map<String, byte[]> au = collection.getOrDefault(artifactId.getAuid(), new HashMap<>());

        try {
            // Convert artifact to WARC record stream
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            // Set unique artifactId
            artifact.getIdentifier().setId(UUID.randomUUID().toString());

            // Create and set the artifact's repository metadata
            RepositoryArtifactMetadata repoMetadata =
        	new RepositoryArtifactMetadata(artifactId, false, false
            );

            artifact.setRepositoryMetadata(repoMetadata);

            // Write artifact as a WARC record stream to the OutputStream
            writeArtifact(artifact, baos);

            //au.put(artifactId.getUri() + String.valueOf(artifactId.getVersion()), baos.toByteArray());
            au.put(artifactId.getId(), baos.toByteArray());
        } catch (HttpException e) {
            throw new IOException(e);
        }

        // Store artifact
        collection.put(artifactId.getAuid(), au);
        repository.put(artifactId.getCollection(), collection);

        return artifact;
    }

    @Override
    public Artifact getArtifact(ArtifactIdentifier artifactId) throws IOException {
        ArtifactIndexData indexedData = index.getArtifactIndexData(artifactId.getId());
        return getVolatileArtifact(indexedData.getCollection(), indexedData.getAuid(), indexedData.getUri(), indexedData.getVersion());
    }

    public Artifact getVolatileArtifact(String collectionId, String auid, String uri, String version) throws IOException {
        // Use identifier to get artifact byte stream
        Map<String, Map<String, byte[]>> collection = repository.get(collectionId);
        Map<String, byte[]> au = collection.get(auid);
        InputStream warcRecordStream = new ByteArrayInputStream(au.get(uri + version));

        // Assemble a WARCRecord object using the WARC record bytestream in memory
        WARCRecord record = new WARCRecord(
                warcRecordStream,
                null,
                0,
                true,
                true
        );

        // Generate an artifact from the HTTP response stream
        Artifact artifact = ArtifactFactory.fromHttpResponseStream(record);

        // TODO: ArtifactFactory#fromHttpResponseStream sets an ArtifactIdentifier if the correct headers are in the
        // HTTP response but since we can't guarantee that yet, we set it explicitly here.
        ArtifactIdentifier artifactId = new ArtifactIdentifier(collectionId, auid, uri, version);
        artifact.setIdentifier(artifactId);

        // Set repository metadata for this artifact
        RepositoryArtifactMetadata repoMetadata = new RepositoryArtifactMetadata(artifactId);

        return artifact;
    }

    public RepositoryArtifactMetadata updateArtifactMetadata(ArtifactIdentifier artifactId, RepositoryArtifactMetadata artifact) {
        // TODO
        return null;
    }

    @Override
    public RepositoryArtifactMetadata commitArtifact(ArtifactIdentifier artifactId) {
       // TODO
        return null;
    }

    @Override
    public RepositoryArtifactMetadata deleteArtifact(ArtifactIdentifier artifactId) {
        ArtifactIndexData indexedData = index.getArtifactIndexData(artifactId.getId());

        Map<String, Map<String, byte[]>> collection = repository.get(indexedData.getCollection());
        Map<String, byte[]> au = collection.get(indexedData.getAuid());
        au.remove(indexedData.getUri() + indexedData.getVersion());

        return null;
    }

}
