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

package org.lockss.laaws.rs.io.storage.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.http.HttpException;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;
import org.archive.io.warc.WARCRecordInfo;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.io.index.VolatileArtifactIndex;
import org.lockss.laaws.rs.io.storage.WarcArtifactStore;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.ArtifactFactory;
import org.springframework.util.DigestUtils;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

public class HdfsWarcArtifactStore extends WarcArtifactStore {
    private final static Log log = LogFactory.getLog(HdfsWarcArtifactStore.class);
    private static final String WARC_FILE_SUFFIX = ".warc";
    public static final String AU_ARTIFACTS_WARC = "artifacts" + WARC_FILE_SUFFIX;

    private Configuration config;
    private Path basePath;
    private FileSystem fs;

    private ArtifactIndex index;

    public HdfsWarcArtifactStore(Configuration config, Path basePath, ArtifactIndex index) {
        this.config = config;
        this.index = index;
        this.basePath = basePath;

        try {
            // Get a FileSystem handle
            this.fs = FileSystem.get(config);
        } catch (IOException e) {
            throw new RuntimeException("Could not get a FileSystem handle with supplied configuration");
        }

        // Make sure the base path directory exists
        mkdirIfNotExist(basePath);

        // Rebuild the index if using volatile index
        if (index.getClass() == VolatileArtifactIndex.class) {
            try {
                rebuildIndex(this.basePath);
            } catch (IOException e) {
                throw new RuntimeException(String.format(
                        "IOException caught while trying to rebuild index from %s",
                        basePath
                ));
            }
        }
    }

    /**
     * Rebuilds the index by traversing a repository base path for artifacts and metadata WARC files.
     *
     * @param basePath The base path of the local repository.
     * @throws IOException
     */
    public void rebuildIndex(Path basePath) throws IOException {
        Collection<Path> warcs = scanDirectories(basePath);

        Collection<Path> artifactWarcFiles = warcs
                .stream()
                .filter(file -> file.getName().endsWith("artifacts" + WARC_FILE_SUFFIX))
                .collect(Collectors.toList());

        // Re-index artifacts first
        for (Path warcFile : artifactWarcFiles) {
            try {
                BufferedInputStream bufferedStream = new BufferedInputStream(fs.open(warcFile));
                for (ArchiveRecord record : WARCReaderFactory.get("HdfsWarcArtifactStore", bufferedStream, true)) {
                    log.info(String.format(
                            "Re-indexing artifact from WARC %s record %s from %s",
                            record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_TYPE),
                            record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_ID),
                            warcFile
                    ));

                    try {
                        Artifact artifact = ArtifactFactory.fromArchiveRecord(record);

                        if (artifact != null) {
                            // Attach repository metadata to artifact
                            artifact.setRepositoryMetadata(new RepositoryArtifactMetadata(
                                    artifact.getIdentifier(),
                                    false,
                                    false
                            ));

                            // Add artifact to the index
                            index.indexArtifact(artifact);
                        }
                    } catch (IOException e) {
                        log.error(String.format(
                                "IOException caught while attempting to re-index WARC record %s from %s",
                                record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_ID),
                                warcFile
                        ));
                    }

                }
            } catch (IOException e) {
                log.error(String.format("IOException caught while attempt to re-index WARC file %s", warcFile));
            }
        }

        // TODO: What follows is loading of artifact repository-specific metadata. It should be generalized to others.

        // Get a collection of repository metadata files
        Collection<Path> repoMetadataWarcFiles = warcs
                .stream()
                .filter(file -> file.getName().endsWith("lockss-repo" + WARC_FILE_SUFFIX))
                .collect(Collectors.toList());

        // Load repository artifact metadata by "replaying" them
        for (Path metadataFile : repoMetadataWarcFiles) {
            try {
                BufferedInputStream bufferedStream = new BufferedInputStream(fs.open(metadataFile));
                for (ArchiveRecord record : WARCReaderFactory.get("HdfsWarcArtifactStore", bufferedStream, true)) {
                    log.info(String.format(
                            "Re-indexing artifact metadata from WARC %s record %s from %s",
                            record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_TYPE),
                            record.getHeader().getHeaderValue(WARCConstants.HEADER_KEY_ID),
                            metadataFile
                    ));

                    // Parse the JSON as a RepositoryArtifactMetadata object
                    RepositoryArtifactMetadata repoStatus = new RepositoryArtifactMetadata(
                            IOUtils.toString(record)
                    );

                    if (index.artifactExists(repoStatus.getArtifactId())) {
                        if (repoStatus.isDeleted()) {
                            log.info(String.format("Removing artifact %s from index", repoStatus.getArtifactId()));
                            index.deleteArtifact(repoStatus.getArtifactId());
                            continue;
                        }

                        if (repoStatus.isCommitted()) {
                            log.info(String.format("Marking aritfact %s as committed in index", repoStatus.getArtifactId()));
                            index.commitArtifact(repoStatus.getArtifactId());
                        }
                    } else {
                        log.warn(String.format("Artifact %s not found in index", repoStatus.getArtifactId()));
                    }
                }
            } catch (IOException e) {
                log.error(String.format(
                        "IOException caught while attempt to re-index metadata WARC file %s",
                        metadataFile
                ));
            }
        }
    }

    /**
     * Recursively finds artifact WARC files under a given base path.
     *
     * @param basePath The base path to scan recursively for WARC files.
     * @return A collection of paths to WARC files under the given base path.
     * @throws IOException
     */
    public Collection<Path> scanDirectories(Path basePath) throws IOException {
        Collection<Path> warcFiles = new ArrayList<>();

//        RemoteIterator<LocatedFileStatus> files = fs.listFiles(basePath, false);
//
//        while (files.hasNext()) {
//            LocatedFileStatus status = files.next();
//            if (status.isDirectory()) {
//                warcFiles.addAll(scanDirectories(status.getPath()));
//            } else {
//                if (status.isFile() && status.getPath().getName().toLowerCase().endsWith(WARC_FILE_SUFFIX))
//                    warcFiles.add(status.getPath());
//            }
//        }

        RemoteIterator<LocatedFileStatus> files = fs.listFiles(basePath, true);
        while(files.hasNext()) {
            LocatedFileStatus status = files.next();
            if (status.isFile() && status.getPath().getName().toLowerCase().endsWith(WARC_FILE_SUFFIX))
                warcFiles.add(status.getPath());
        }

        // Return WARC files at this level
        return warcFiles;
    }

    /**
     * Returns the filesystem base path to the archival unit (AU) this artifact belongs in.
     *
     * @param artifactId Artifact identifier of an artifact.
     * @return Base path of the AU the artifact belongs in.
     */
    public Path getArchicalUnitBasePath(ArtifactIdentifier artifactId) {
        String auidHash = DigestUtils.md5DigestAsHex(artifactId.getAuid().getBytes());
        Path auPath = new Path(getCollectionBasePath(artifactId) + SEPARATOR + AU_DIR_PREFIX + auidHash);
        mkdirIfNotExist(auPath);
        return auPath;
    }

    /**
     * Returns the filesystem base path to the collection this artifact belongs in.
     *
     * @param artifactId Artifact identifier of an artifact.
     * @return Base path of the collection the artifact belongs in.
     */
    public Path getCollectionBasePath(ArtifactIdentifier artifactId) {
        Path collectionDir = new Path(this.basePath + SEPARATOR + artifactId.getCollection());
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
    public void mkdirIfNotExist(Path path) {
        try {
            if (fs.exists(path)) {
                // YES: Make sure it is a directory
                if (!fs.isDirectory(path)) {
                    throw new RuntimeException(String.format("%s exists but is not a directory", path));
                }
            } else {
                // NO: Create a directory for the collection
                fs.mkdirs(path);
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format(
                    "Caught IOException while creating or verifying %s is a directory",
                    path
            ));
        }
    }

    public void createWarcFile(Path warcFilePath) throws IOException {
        if (!fs.exists(warcFilePath)) {
            // Create a new WARC file
            fs.createNewFile(warcFilePath);

            // TODO: Write a warcinfo WARC record

        } else {
            if (!fs.isFile(warcFilePath)) {
                log.warn(String.format("%s is not a file", warcFilePath));
            }
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
            Path auBasePath = getArchicalUnitBasePath(artifactId);
            Path auArtifactsWarcPath = new Path(auBasePath + SEPARATOR + AU_ARTIFACTS_WARC);

            // Make sure the WARC file exists
            createWarcFile(auArtifactsWarcPath);

            // Set the offset for the record to be appended to the length of the WARC file (i.e., the end)
            long offset = fs.getFileStatus(auArtifactsWarcPath).getLen();

            // Get an appending OutputStream to the WARC file
            FSDataOutputStream fos = fs.append(auArtifactsWarcPath);

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
            artifact.setRepositoryMetadata(new RepositoryArtifactMetadata(
                    artifactId,
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
     * @throws URISyntaxException 
     */
    @Override
    public Artifact getArtifact(ArtifactIdentifier artifactId)
	throws IOException, URISyntaxException {
        log.info(String.format("Retrieving artifact from store (artifactId: %s)", artifactId.toString()));

        // Get details from the artifact index service
        ArtifactIndexData indexedData = index.getArtifactIndexData(artifactId.getId());

        // TODO: Remove - only for debugging
        log.info(indexedData.toString());

        URI uri = new URI(indexedData.getStorageUrl());

        // Get InputStream to WARC file
        String warcFilePath =
            uri.getScheme() + uri.getAuthority() + uri.getPath();
        FSDataInputStream warcStream = fs.open(new Path(warcFilePath));

        // Seek to the WARC record
        List<String> offsetQueryArgs = UriComponentsBuilder
            .fromUri(uri).build().getQueryParams().get("offset");

        if (offsetQueryArgs != null && !offsetQueryArgs.isEmpty()) {
          warcStream.seek(Long.parseLong(offsetQueryArgs.get(0)));
        }

        // Get a WARCRecord object
        WARCRecord record = new WARCRecord(warcStream, "HdfsWarcArtifactStore#getArtifact", 0);

        // Convert the WARCRecord object to an Artifact
        Artifact artifact = ArtifactFactory.fromArchiveRecord(record);

        // Set artifact's repository metadata
        RepositoryArtifactMetadata repoMetadata = new RepositoryArtifactMetadata(
                artifactId,
                indexedData.getCommitted(),
                false
        );

        artifact.setRepositoryMetadata(repoMetadata);

        // Close the stream
        warcStream.close();

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
    public RepositoryArtifactMetadata updateArtifactMetadata(ArtifactIdentifier artifactId, RepositoryArtifactMetadata metadata) throws IOException {

//        if (!isDeleted(artifactId)) {
        // Convert ArtifactMetadata object into a WARC metadata record
        WARCRecordInfo metadataRecord = createWarcMetadataRecord(
//                    getWarcRecordId(indexedData.getWarcFilePath(), indexedData.getWarcRecordOffset()),
                artifactId.getId(),
                metadata
        );

        // Get an OutputStream to the AU's metadata file
        Path metadataFilePath = new Path(getArchicalUnitBasePath(artifactId) + SEPARATOR + metadata.getMetadataId() + WARC_FILE_SUFFIX);

        // Make sure the WARC file exists
        createWarcFile(metadataFilePath);

        FSDataOutputStream fos = fs.append(metadataFilePath);

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
     * @throws IOException
     * @throws URISyntaxException 
     */
    @Override
    public RepositoryArtifactMetadata commitArtifact(ArtifactIdentifier artifactId)
	throws IOException, URISyntaxException {
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
     * Marks the artifact as deleted in the repository by updating the repository metadata for this artifact.
     *
     * @param artifactId The artifact identifier of the artifact to mark as deleted.
     * @throws IOException
     * @throws URISyntaxException 
     */
    @Override
    public RepositoryArtifactMetadata deleteArtifact(ArtifactIdentifier artifactId)
	throws IOException, URISyntaxException {
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
