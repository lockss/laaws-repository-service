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

package org.lockss.laaws.rs.io.storage;

import com.google.common.io.CountingOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.http.HttpException;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCRecord;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.util.ArchiveUtils;
import org.archive.util.anvl.Element;
import org.lockss.laaws.rs.io.index.ArtifactIndex;
import org.lockss.laaws.rs.model.*;
import org.lockss.laaws.rs.util.ArtifactConstants;
import org.lockss.laaws.rs.util.ArtifactUtil;
import org.springframework.util.MultiValueMap;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.UUID;

public abstract class WarcArtifactStore implements ArtifactStore, WARCConstants {
    private final static Log log = LogFactory.getLog(WarcArtifactStore.class);

    public static final String AU_DIR_PREFIX = "au-";
    private static final String SCHEME = "urn:uuid";
    private static final String SCHEME_COLON = SCHEME + ":";
    public static final String CRLF = "\r\n";
    public static byte[] CRLF_BYTES;
    public static String SEPARATOR = "/";

    protected ArtifactIndex index;
    public File repositoryBasePath;

    static {
        try {
            CRLF_BYTES = CRLF.getBytes(DEFAULT_ENCODING);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the WARC-Record-Id of the WARC record backing a given Artifact.
     *
     * @param file URL to a WARC file.
     * @param offset Absolute byte offset of WARC record in WARC file.
     * @return The value of the mandatory WARC-Record-Id header.
     * @throws IOException
     */
    public static String getWarcRecordId(URL file, long offset) throws IOException {
        WARCRecord record = getWarcRecord(file, offset);
        ArchiveRecordHeader recordHeader = record.getHeader();
        return (String) recordHeader.getHeaderValue(WARCConstants.HEADER_KEY_ID);
    }

    /**
     * Returns a WARCRecord object representing the WARC record at a given URL and offset.
     *
     * Support for different protocols is handled by implementing URLConnection, URLStreamHandler, and
     * URLStreamHandlerFactory, then registering the custom URLStreamHandlerFactory with URL#setURLStreamHandlerFactory.
     *
     * @param file URL to a WARC file.
     * @param offset Absolute byte offset of WARC record in WARC file.
     * @return A WARCRecord object representing the WARC record in the WARC file, at the given offset.
     * @throws IOException
     */
    public static WARCRecord getWarcRecord(URL file, long offset) throws IOException {
        InputStream warcStream = file.openStream();
        warcStream.skip(offset);
        return new WARCRecord(file.openStream(), "WarcArtifactStore", 0);
    }

    /**
     * Creates a WARCRecordInfo object representing a WARC metadata record with a JSON object as its payload.
     *
     * @param refersTo The WARC-Record-Id of the WARC record this metadata is attached to (i.e., for WARC-Refers-To).
     * @param metadata A RepositoryArtifactMetadata with the artifact metadata.
     * @return A WARCRecordInfo representing the given artifact metadata.
     */
    public static WARCRecordInfo createWarcMetadataRecord(String refersTo, RepositoryArtifactMetadata metadata) {
        // Create a WARC record object
        WARCRecordInfo record = new WARCRecordInfo();

        // Set record content stream
        byte[] metadataBytes = metadata.toJson().toString().getBytes();
        record.setContentStream(new ByteArrayInputStream(metadataBytes));

        // Mandatory WARC record headers
        record.setRecordId(URI.create(UUID.randomUUID().toString()));
        record.setCreate14DigitDate(DateTimeFormatter.ISO_INSTANT.format(Instant.now().atZone(ZoneOffset.UTC)));
        record.setType(WARCRecordType.metadata);
        record.setContentLength(metadataBytes.length);
        record.setMimetype(MimeType.JSON);

        // Set the WARC-Refers-To field to the WARC-Record-ID of the artifact
        record.addExtraHeader(WARCConstants.HEADER_KEY_REFERS_TO, refersTo);

        return record;
    }

    /**
     * Writes an artifact as a WARC record to a given OutputStream.
     *
     * @param artifact Artifact to add to the repository.
     * @param outputStream OutputStream to write the WARC record representing this artifact.
     * @return The number of bytes written to the WARC file for this record.
     * @throws IOException
     * @throws HttpException
     */
    public static long writeArtifact(Artifact artifact, OutputStream outputStream) throws IOException, HttpException {
        // Get artifact identifier
        ArtifactIdentifier artifactId = artifact.getIdentifier();

        // Create a WARC record object
        WARCRecordInfo record = new WARCRecordInfo();

        // Mandatory WARC record headers
//        record.setRecordId(URI.create(UUID.randomUUID().toString()));
        record.setRecordId(URI.create(artifactId.getId()));
        record.setCreate14DigitDate(artifactId.getVersion());
        record.setType(WARCRecordType.response);

        // Optional WARC record headers
        record.setUrl(artifactId.getUri());
        record.setMimetype("application/http; msgtype=response"); // Content-Type of WARC payload

        // Add LOCKSS-specific WARC headers to record (Note: X-Lockss-ArtifactId and X-Lockss-Uri are redundant because
        // the same information is recorded as WARC-Record-ID and WARC-Target-URI, respectively).
        record.addExtraHeader(ArtifactConstants.ARTIFACTID_ID_KEY, artifactId.getId());
        record.addExtraHeader(ArtifactConstants.ARTIFACTID_COLLECTION_KEY, artifactId.getCollection());
        record.addExtraHeader(ArtifactConstants.ARTIFACTID_AUID_KEY, artifactId.getAuid());
        record.addExtraHeader(ArtifactConstants.ARTIFACTID_URI_KEY, artifactId.getUri());
        record.addExtraHeader(ArtifactConstants.ARTIFACTID_VERSION_KEY, artifactId.getVersion());

        // We must determine the size of the WARC payload (which is an artifact encoded as an HTTP response stream)
        // but it is not possible to determine the final size without reading the InputStream entirely, so we use a
        // DeferredFileOutputStream, copy the InputStream into it, and determine the number of bytes written.
        DeferredFileOutputStream dfos = new DeferredFileOutputStream(1048576, "writeArtifactDfos", null, new File("/tmp"));
        IOUtils.copy(ArtifactUtil.getHttpResponseStreamFromArtifact(artifact), dfos);
        dfos.close();

        // Attach WARC record payload
        record.setContentStream(dfos.isInMemory() ? new ByteArrayInputStream(dfos.getData()) : new FileInputStream(dfos.getFile()));
        record.setContentLength(dfos.getByteCount());

        // Write WARCRecordInfo to OutputStream
        CountingOutputStream cout = new CountingOutputStream(outputStream);
        writeWarcRecord(record, cout);
        return cout.getCount();
    }

    /**
     * Writes a WARC record to a given OutputStream.
     *
     * @param record An instance of WARCRecordInfo to write to the OutputStream.
     * @param out An OutputStream.
     * @throws IOException
     */
    public static void writeWarcRecord(WARCRecordInfo record, OutputStream out) throws IOException {
        // Write the header
        out.write(createRecordHeader(record).getBytes(WARC_HEADER_ENCODING));

        // Write the header-body separator
        out.write(CRLF_BYTES);

        // Write the body
        if (record.getContentStream() != null) {
            // TODO: Put logic here to check and enforce WARC file lengths
            int bytesWritten = IOUtils.copy(record.getContentStream(), out);
            if (bytesWritten != record.getContentLength())
                log.warn(String.format("Expected %d bytes, but wrote %d", record.getContentLength(), bytesWritten));
        }

        // Write the two blank lines at end of all records
        out.write(CRLF_BYTES);
        out.write(CRLF_BYTES);
    }

    /**
     * Composes a String object containing WARC record header of a given WARCRecordInfo.
     *
     * @param record A WARCRecordInfo representing a WARC record.
     * @return The header for this WARCRecordInfo.
     */
    public static String createRecordHeader(WARCRecordInfo record) {
        final StringBuilder sb = new StringBuilder();

        // WARC record identifier
        sb.append(WARC_ID).append(CRLF);

        // WARC record mandatory headers
        sb.append(HEADER_KEY_ID).append(COLON_SPACE).append('<').append(SCHEME_COLON).append(record.getRecordId().toString()).append('>').append(CRLF);
//        sb.append(HEADER_KEY_ID).append(COLON_SPACE).append(record.getRecordId().toString()).append(CRLF);
        sb.append(CONTENT_LENGTH).append(COLON_SPACE).append(Long.toString(record.getContentLength())).append(CRLF);
        sb.append(HEADER_KEY_DATE).append(COLON_SPACE).append(record.getCreate14DigitDate()).append(CRLF);
        sb.append(HEADER_KEY_TYPE).append(COLON_SPACE).append(record.getType()).append(CRLF);

        // Optional WARC-Target-URI
        if (!StringUtils.isEmpty(record.getUrl()))
            sb.append(HEADER_KEY_URI).append(COLON_SPACE).append(record.getUrl()).append(CRLF);

        // Optional Content-Type of WARC record payload
        if (record.getContentLength() > 0)
            sb.append(CONTENT_TYPE).append(COLON_SPACE).append(record.getMimetype()).append(CRLF);

        // Extra WARC record headers
        if (record.getExtraHeaders() != null) {
//            record.getExtraHeaders().stream().map(x -> sb.append(x).append(CRLF));

            for (final Iterator<Element> i = record.getExtraHeaders().iterator(); i.hasNext();) {
                sb.append(i.next()).append(CRLF);
            }
        }

        return sb.toString();
    }

    /**
     * Creates a warcinfo type WARC record with metadata common to all following WARC records.
     *
     * Adapted from iipc/webarchive-commons.
     *
     * @param extraHeaders
     * @return
     */
    public static WARCRecordInfo createWARCInfoRecord(MultiValueMap<String, String> extraHeaders) {
        WARCRecordInfo record = new WARCRecordInfo();
        record.setRecordId(generateRecordId());
        record.setType(WARCRecordType.warcinfo);
        record.setCreate14DigitDate(ArchiveUtils.get14DigitDate());

        // Set extra headers
        extraHeaders.forEach((k, vs) -> vs.forEach(v -> record.addExtraHeader(k, v)));

//        record.setMimetype("application/warc-fields");
//        byte[] contents = "Not implemented".getBytes(UTF8);
//        record.setContentStream(new ByteArrayInputStream(contents));
//        record.setContentLength((long)contents.length);
        record.setContentLength(0);

        return record;
    }

    /**
     * Generates a new UUID for use as the WARC-Record-Id for a WARC record.
     *
     * @return A new UUID.
     */
    private static URI generateRecordId() {
        try {
            return new URI(SCHEME_COLON + UUID.randomUUID().toString());
        } catch (URISyntaxException e) {
            // This should never happen
            throw new RuntimeException(e);
        }
    }
}
