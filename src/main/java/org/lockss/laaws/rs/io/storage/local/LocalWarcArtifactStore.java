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

package org.lockss.laaws.rs.io.storage.local;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCReaderFactory;
import org.lockss.laaws.rs.io.storage.WarcArtifactStore;
import org.lockss.laaws.rs.util.ArtifactFactory;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.model.ArtifactIndexData;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIdentifier;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class LocalWarcArtifactStore extends WarcArtifactStore {
    private static final Log log = LogFactory.getLog(LocalWarcArtifactStore.class);
    private static final String WARC_FILE_SUFFIX = ".warc";

    private ArtifactIndex index;
    private File baseDir;

    public LocalWarcArtifactStore(ArtifactIndex index, File baseDir) {
        log.info(String.format("Loading all WARCs under %s", baseDir.getAbsolutePath()));

        this.index = index;
        this.baseDir = baseDir;

        // Rebuild the index
        if (baseDir.exists() && baseDir.isDirectory()) {
            Collection<File> warcs = scanDirectories(baseDir);
            warcs.stream().forEach(this::addWARC);
        }
    }

    public static Collection<File> scanDirectories(File warcFilesDir) {
        Collection<File> warcFiles = new ArrayList<>();

        // DFS recursion through directories
        Arrays.stream(warcFilesDir.listFiles(x -> x.isDirectory()))
                .map(x -> scanDirectories(x))
                .forEach(warcFiles::addAll);

        // Add WARC files from this directory
        warcFiles.addAll(Arrays.asList(warcFilesDir.listFiles((dir, name) ->
            new File(dir, name).isFile() && name.toLowerCase().endsWith(WARC_FILE_SUFFIX)
        )));

        // Return WARC files at this level
        return warcFiles;
    }

    public void addWARC(File warcFile) {
        try {
            for (ArchiveRecord record : WARCReaderFactory.get(warcFile)) {
                ArchiveRecordHeader headers = record.getHeader();
                String recordType = (String) headers.getHeaderValue(WARCConstants.HEADER_KEY_TYPE);

                // Log what we're doing
                log.info(String.format(
                        "Importing WARC record (ID: %s, type: %s), headers: %s",
                        record.getHeader().getHeaderValue("WARC-Record-ID"),
                        recordType,
                        record.getHeader()
                ));

                // Convert ArchiveRecord to Artifact and add it to the index
                Artifact artifact = ArtifactFactory.fromArchiveRecord(record);

                if (artifact != null) {
                    // Assign the artifact an identity
                    artifact.setIdentifier(new ArtifactIdentifier(
                            "collection1",
                            "auid",
                            headers.getUrl(),
                            headers.getVersion()
                    ));

                    // Add the artifact
                    addArtifact(artifact);
                }
            }
        } catch (IOException e) {
            log.error(String.format("An IO exception occurred while iterating over records from %s: %s", warcFile, e));
        }
    }

    @Override
    public ArtifactIdentifier addArtifact(Artifact artifact) throws IOException {
        if (index == null) {
            // YES: Cannot proceed without an artifact index - throw RuntimeException
            throw new RuntimeException("No artifact index");
        } else {
            // NO: Add the Artifact to the index
            ArtifactIdentifier id = artifact.getIdentifier();
            log.info(String.format(
                    "Indexing artifact (%s, %s, %s, %s)",
                    id.getCollection(),
                    id.getAuid(),
                    id.getUri(),
                    id.getVersion()
            ));

            //// Add the artifact to the storage

            // Create a directory for this collection if one doesn't exist
            if (baseDir.exists()) {
                if (!baseDir.isDirectory()) {
                    throw new RuntimeException("Base directory path must point to a directory");
                }

                baseDir.mkdir();
            }

            // Open the WARC file for this AU

            // Write artifact to WARC
            try {
                this.writeArtifact(artifact, null);
            } catch (HttpException e) {

            }

            // Add the artifact to the index
            ArtifactIndexData data = index.indexArtifact(artifact);
            index.commitArtifact(data.getId());
        }

        // Return a pointer to the artifact
        return artifact.getIdentifier();
    }

    @Override
    public Artifact getArtifact(ArtifactIndexData indexData) throws IOException {
        return null;
    }

    @Override
    public void updateArtifact(ArtifactIndexData indexData, Artifact artifact) {

    }

    @Override
    public void deleteArtifact(ArtifactIndexData indexData) {

    }
}
