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

package org.lockss.laaws.rs.io.storage.mock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.archive.io.warc.WARCRecord;
import org.lockss.laaws.rs.util.ArtifactFactory;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.model.ArtifactIndexData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.io.storage.WARCArtifactStore;
import org.lockss.laaws.rs.model.Artifact;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class VolatileWARCArtifactStore extends WARCArtifactStore {
    private final static Log log = LogFactory.getLog(VolatileWARCArtifactStore.class);
    private Map<String, Map<String, Map<String, byte[]>>> repository;

    public VolatileWARCArtifactStore() {
        this.repository = new HashMap<>();
    }

    @Override
    public ArtifactIdentifier addArtifact(Artifact artifact) throws IOException {
        // Get artifact identifier
        ArtifactIdentifier aid = artifact.getIdentifier();

        // Get the collection
        Map<String, Map<String, byte[]>> collection = repository.getOrDefault(aid.getCollection(), new HashMap<>());

        // Get the AU
        Map<String, byte[]> au = collection.getOrDefault(aid.getAuid(), new HashMap<>());

        try {
            // Convert artifact to WARC record stream
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            writeArtifact(artifact, baos);
            au.put(aid.getUri() + String.valueOf(aid.getVersion()), baos.toByteArray());
        } catch (HttpException e) {
            throw new IOException(e);
        }

        // Store artifact
        collection.put(aid.getAuid(), au);
        repository.put(aid.getCollection(), collection);

        return aid;
    }

    @Override
    public Artifact getArtifact(ArtifactIndexData indexedData) throws IOException {
        // TODO: Should this throw an exception instead?
        if (indexedData == null)
            return null;

        return getVolatileArtifact(indexedData.getCollection(), indexedData.getAuid(), indexedData.getUri(), indexedData.getVersion());
    }

    public Artifact getVolatileArtifact(String collectionId, String auid, String uri, String version) throws IOException {
        // Use identifier to get artifact byte stream
        Map<String, Map<String, byte[]>> collection = repository.get(collectionId);
        Map<String, byte[]> au = collection.get(auid);
        InputStream warcRecordStream = new ByteArrayInputStream(au.get(uri + version));

        WARCRecord record = new WARCRecord(
                warcRecordStream,
                null,
                0,
                true,
                true
        );

        // Generate an artifact from the HTTP response stream
        Artifact artifact = ArtifactFactory.fromHttpResponseStream(record);

        // Do we still need this?
        ArtifactIdentifier identifier = new ArtifactIdentifier(collectionId, auid, uri, version);
        artifact.setIdentifier(identifier);

        // Return artifact
        return artifact;
    }

    @Override
    public void updateArtifact(ArtifactIndexData indexData, Artifact artifact) {
        // Intentionally left blank
    }

    @Override
    public void deleteArtifact(ArtifactIndexData indexData) {
        Map<String, Map<String, byte[]>> collection = repository.get(indexData.getCollection());
        Map<String, byte[]> au = collection.get(indexData.getAuid());
        au.remove(indexData.getUri() + indexData.getVersion());
    }

}
