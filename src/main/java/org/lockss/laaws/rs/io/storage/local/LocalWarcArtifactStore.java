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
import org.archive.io.warc.WARCRecord;
import org.archive.io.warc.WARCRecordInfo;
import org.lockss.laaws.rs.io.storage.WarcArtifactStore;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.ArtifactFactory;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.springframework.util.DigestUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

public class LocalWarcArtifactStore extends WarcArtifactStore {
    private static final Log log = LogFactory.getLog(LocalWarcArtifactStore.class);

    private static final String WARC_FILE_SUFFIX = ".warc";
    public static final String AU_ARTIFACTS_WARC = "artifacts" + WARC_FILE_SUFFIX;

    /**
     * Constructor. Rebuilds the index on start-up from a given repository base path.
     *
     * @param index              An instance of ArtifactIndex to use for indexing artifacts.
     * @param repositoryBasePath The base path of the repository's WARC file directory structure.
     */
    public LocalWarcArtifactStore(ArtifactIndex index, File repositoryBasePath) {
        log.info(String.format("Loading all WARCs under %s", repositoryBasePath.getAbsolutePath()));

        this.index = index;
        this.repositoryBasePath = repositoryBasePath;

        // Rebuild the index
//        if (repositoryBasePath.exists() && repositoryBasePath.isDirectory()) {
//            Collection<File> warcs = scanDirectories(repositoryBasePath);
//            warcs.stream().forEach(this::addWARC);
//        }
    }

    /**
     * Recursively finds all WARC files under a given base path.
     *
     * @param basePath The base path to scan recursively for WARC files.
     * @return A collection of paths to WARC files under the given base path.
     */
    public static Collection<File> scanDirectories(File basePath) {
        Collection<File> warcFiles = new ArrayList<>();

        // DFS recursion through directories
        Arrays.stream(basePath.listFiles(x -> x.isDirectory()))
                .map(x -> scanDirectories(x))
                .forEach(warcFiles::addAll);

        // Add WARC files from this directory
        warcFiles.addAll(Arrays.asList(basePath.listFiles((dir, name) ->
                new File(dir, name).isFile() && name.toLowerCase().endsWith(WARC_FILE_SUFFIX)
        )));

        // Return WARC files at this level
        return warcFiles;
    }

    /**
     * Adds the WARC records in WARC file from given path, to the repository.
     *
     * @param warcFile Path to a WARC file to process.
     */
    public void addWARC(File warcFile) {
        try {
            for (ArchiveRecord record : WARCReaderFactory.get(warcFile)) {
                // Get WARC record headers
                ArchiveRecordHeader headers = record.getHeader();
                String recordId = (String) headers.getHeaderValue(WARCConstants.HEADER_KEY_ID);
                String recordType = (String) headers.getHeaderValue(WARCConstants.HEADER_KEY_TYPE);

                log.info(String.format(
                        "Importing WARC record (ID: %s, type: %s), headers: %s",
                        record.getHeader().getHeaderValue("WARC-Record-ID"),
                        recordType,
                        record.getHeader()
                ));

                try {
                    // Convert ArchiveRecord to Artifact
                    Artifact artifact = ArtifactFactory.fromArchiveRecord(record);

                    // If we got an Artifact object without errors, add it to the repository
                    if (artifact != null) {
                        // TODO: Assign the artifact an identity - this must come from WARC record
                        artifact.setIdentifier(new ArtifactIdentifier(
                                "collection1",
                                "auid",
                                headers.getUrl(),
                                headers.getVersion(),
                                recordId
                        ));

                        // Add artifact to repository
                        addArtifact(artifact);
                    }
                } catch (IOException e) {
                    log.error(String.format(
                            "An IO exception occurred while attempting to add WARC record id %s from %s: %s",
                            recordId,
                            warcFile,
                            e)
                    );
                }
            }
        } catch (IOException e) {
            log.error(String.format("An IO exception occurred while iterating over records from %s: %s", warcFile, e));
        }
    }

    /**
     * Returns the filesystem base path to the archival unit (AU) this artifact belongs in.
     *
     * @param artifactId Artifact identifier of an artifact.
     * @return Base path of the AU the artifact belongs in.
     */
    public File getArchicalUnitBasePath(ArtifactIdentifier artifactId) {
        String auidHash = DigestUtils.md5DigestAsHex(artifactId.getAuid().getBytes());
        File auPath = new File(getCollectionBasePath(artifactId) + SEPARATOR + AU_DIR_PREFIX + auidHash);
        mkdirIfNotExist(auPath);
        return auPath;
    }

    /**
     * Returns the filesystem base path to the collection this artifact belongs in.
     *
     * @param artifactId Artifact identifier of an artifact.
     * @return Base path of the collection the artifact belongs in.
     */
    public File getCollectionBasePath(ArtifactIdentifier artifactId) {
        File collectionDir = new File(repositoryBasePath + SEPARATOR + artifactId.getCollection());
        mkdirIfNotExist(collectionDir);
        return collectionDir;
    }

    /**
     * Ensures a directory exists at the given path by creating one if nothing exists there. Throws a
     * RunTimeExceptionError if something exists at the path but is not a directory, because there is no way to safely
     * recover from this situation.
     *
     * @param path Path to the directory to create, if it doesn't exist yet.
     */
    public static void mkdirIfNotExist(File path) {
        if (path.exists()) {
            // YES: Make sure it is a directory
            if (!path.isDirectory()) {
                throw new RuntimeException(String.format("%s exists but is not a directory", path));
            }
        } else {
            // NO: Create a directory for the collection
            path.mkdir();
        }
    }

    /**
     * Adds an artifact to the repository.
     *
     * @param artifact An artifact.
     * @return An artifact identifier for artifact reference within this repository.
     * @throws IOException
     */
    @Override
    public Artifact addArtifact(Artifact artifact) throws IOException {
        if (index == null) {
            // YES: Cannot proceed without an artifact index - throw RuntimeException
            throw new RuntimeException("No artifact index configured!");

        } else {
            // NO: Add the Artifact to the index
            ArtifactIdentifier artifactId = artifact.getIdentifier();

            // Set new artifactId - any existing artifactId is meaningless in this context and should be discarded
            artifactId.setId(UUID.randomUUID().toString());

            log.info(String.format(
                    "Adding artifact (%s, %s, %s, %s, %s)",
                    artifactId.getId(),
                    artifactId.getCollection(),
                    artifactId.getAuid(),
                    artifactId.getUri(),
                    artifactId.getVersion()
            ));

            // Get an OutputStream
            File auBasePath = getArchicalUnitBasePath(artifactId);
            File auArtifactsWarcPath = new File(auBasePath + SEPARATOR + AU_ARTIFACTS_WARC);

            // Set the offset for the record to be appended to the length of the WARC file (i.e., the end)
            long offset = auArtifactsWarcPath.length();

            // Get an appending OutputStream to the WARC file
            FileOutputStream fos = new FileOutputStream(auArtifactsWarcPath, true);

            try {
                // Write artifact to WARC file
                long bytesWritten = this.writeArtifact(artifact, fos);

                // Calculate offset of next record
//                offset += bytesWritten;
            } catch (HttpException e) {
                throw new IOException(
                        String.format("Caught HttpException while attempting to write artifact to WARC file: %s", e)
                );
            }

            // Close the file
            fos.flush();
            fos.close();

            // Attach the artifact's repository metadata
            artifact.setRepositoryMetadata(new WarcRepositoryArtifactMetadata(
                    artifactId,
                    auArtifactsWarcPath.toString(),
                    offset,
                    false,
                    false
            ));

            // TODO: Generalize this to write all of an artifact's metadata
            updateArtifactMetadata(artifactId, artifact.getRepositoryMetadata());

            // Add the artifact to the index
            index.indexArtifact(artifact);
        }

        // Return the artifact
        return artifact;
    }

    /**
     * Retrieves an artifact from the repository storage.
     *
     * @param artifactId The artifact identifier of artifact to retrieve.
     * @return An artifact.
     * @throws IOException
     */
    @Override
    public Artifact getArtifact(ArtifactIdentifier artifactId) throws IOException {
        log.info(String.format("Retrieving artifact from store (artifactId: %s)", artifactId.toString()));

        // Get details from the artifact index service
        ArtifactIndexData indexedData = index.getArtifactIndexData(artifactId.getId());

        // TODO: Remove - only for debugging
        log.info(indexedData.toString());

        // Get InputStream to WARC file
        InputStream warcStream = new FileInputStream(indexedData.getWarcFilePath());

        // Seek to the WARC record
        warcStream.skip(indexedData.getWarcRecordOffset());

        // Get a WARCRecord object
        WARCRecord record = new WARCRecord(warcStream, "LocalWarcArtifactStore#getArtifact", 0);

        // Convert the WARCRecord object to an Artifact
        Artifact artifact = ArtifactFactory.fromArchiveRecord(record);

        // Set artifact's repository metadata
        WarcRepositoryArtifactMetadata repoMetadata = new WarcRepositoryArtifactMetadata(
                artifactId,
                indexedData.getWarcFilePath(),
                indexedData.getWarcRecordOffset(),
                indexedData.getCommitted(),
                false
        );

        artifact.setRepositoryMetadata(repoMetadata);

        // Return an Artifact from the WARC record
        return artifact;
    }

    /**
     * Updates the metadata of an artifact by appending a WARC metadata record to a metadata WARC file.
     *
     * @param artifactId The artifact identifier to add the metadata to.
     * @param metadata   Artifact metadata.
     * @throws IOException
     */
    @Override
    public ArtifactMetadata updateArtifactMetadata(ArtifactIdentifier artifactId, ArtifactMetadata metadata) throws IOException {

//        if (!isDeleted(artifactId)) {
            // Convert ArtifactMetadata object into a WARC metadata record
            WARCRecordInfo metadataRecord = createWarcMetadataRecord(
//                    getWarcRecordId(indexedData.getWarcFilePath(), indexedData.getWarcRecordOffset()),
                    artifactId.getId(),
                    metadata
            );

            // Get an OutputStream to the AU's metadata file
            String metadataPath = getArchicalUnitBasePath(artifactId) + SEPARATOR + metadata.getMetadataId() + WARC_FILE_SUFFIX;
            FileOutputStream fos = new FileOutputStream(metadataPath, true);

            // Append WARC metadata record to AU's repository metadata file
            writeWarcRecord(metadataRecord, fos);

            // Close the OutputStream
            fos.close();
//        }
        return metadata;
    }

    /**
     * Marks the artifact as committed in the repository by updating the repository metadata for this artifact, and the
     * committed status in the artifact index.
     *
     * @param artifactId The artifact identifier of the artifact to commit.
     */
    @Override
    public RepositoryArtifactMetadata commitArtifact(ArtifactIdentifier artifactId) throws IOException {
        Artifact artifact = getArtifact(artifactId);
        RepositoryArtifactMetadata repoMetadata = artifact.getRepositoryMetadata();

        // Set the commit flag and write the metadata to disk
        if (!repoMetadata.isDeleted()) {
            repoMetadata.setCommitted(true);
            updateArtifactMetadata(artifactId, repoMetadata);

            // Update the committed flag in the index
            index.commitArtifact(artifactId.getId());
        }

        return repoMetadata;
    }

    /**
     * Returns a boolean indicating whether an artifact is marked as deleted in the repository storage.
     *
     * @param artifactId The artifact identifier of the aritfact to check.
     * @return A boolean indicating whether the artifact is marked as deleted.
     * @throws IOException
     */
    public boolean isDeleted(ArtifactIdentifier artifactId) throws IOException {
        Artifact artifact = getArtifact(artifactId);
        RepositoryArtifactMetadata metadata = artifact.getRepositoryMetadata();
        return metadata.isDeleted();
    }

    /**
     * Returns a boolean indicating whether an artifact is marked as committed in the repository storage.
     *
     * @return A boolean indicating whether the artifact is marked as committed.
     */
    public boolean isCommitted(ArtifactIdentifier artifactId) throws IOException {
        Artifact artifact = getArtifact(artifactId);
        RepositoryArtifactMetadata metadata = artifact.getRepositoryMetadata();
        return metadata.isCommitted();
    }

    /**
     * Marks the artifact as deleted in the repository by updating the repository metadata for this artifact.
     *
     * @param artifactId The artifact identifier of the artifact to mark as deleted.
     */
    @Override
    public RepositoryArtifactMetadata deleteArtifact(ArtifactIdentifier artifactId) throws IOException {
        Artifact artifact = getArtifact(artifactId);
        RepositoryArtifactMetadata repoMetadata = artifact.getRepositoryMetadata();

        if (!repoMetadata.isDeleted()) {
            // Update the repository metadata
            repoMetadata.setCommitted(false);
            repoMetadata.setDeleted(true);

            // Write to disk
            updateArtifactMetadata(artifactId, repoMetadata);

            // Update the committed flag in the index
            index.commitArtifact(artifactId.getId());
        }

        return repoMetadata;
    }
}