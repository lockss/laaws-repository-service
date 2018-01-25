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

package org.lockss.laaws.rs.io.storage;

import com.google.common.io.CountingOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.DeferredFileOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.archive.format.warc.WARCConstants;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.util.anvl.Element;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.util.ArtifactUtil;
import org.lockss.laaws.rs.model.Artifact;

import java.io.*;
import java.net.URI;
import java.util.Iterator;
import java.util.UUID;

public abstract class WARCArtifactStore implements ArtifactStore, WARCConstants {
    private final static Log log = LogFactory.getLog(WARCArtifactStore.class);

    public static final String CRLF = "\r\n";
    public static byte[] CRLF_BYTES;

    static {
        try {
            CRLF_BYTES = CRLF.getBytes(DEFAULT_ENCODING);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public long writeArtifact(Artifact artifact, OutputStream outputStream) throws IOException, HttpException {
        // Get artifact identifier
        ArtifactIdentifier identifier = artifact.getIdentifier();

        // Create a WARC record object
        WARCRecordInfo record = new WARCRecordInfo();

        // Mandatory WARC record headers
        record.setRecordId(URI.create(UUID.randomUUID().toString()));
        record.setCreate14DigitDate(identifier.getVersion());
        record.setType(WARCConstants.WARCRecordType.response);

        // Optional WARC record headers
        record.setUrl(identifier.getUri());
        record.setMimetype("application/http; msgtype=response"); // Content-Type of WARC payload

        // Add LOCKSS-specific WARC headers to record
        record.addExtraHeader("X-Lockss-Collection", identifier.getCollection());
        record.addExtraHeader("X-Lockss-AuId", identifier.getAuid());
        record.addExtraHeader("X-Lockss-Uri", identifier.getUri());
        record.addExtraHeader("X-Lockss-Version", identifier.getVersion());

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
        write(record, cout);
        return cout.getCount();
    }

    public static void write(WARCRecordInfo record, OutputStream out) throws IOException {
        // Write the header
        out.write(createRecordHeader(record).getBytes(WARC_HEADER_ENCODING));

        // Write the header-body separator
        out.write(CRLF_BYTES);

        // Write the body
        if (record.getContentStream() != null) {
            int bytesWritten = IOUtils.copy(record.getContentStream(), out);
            if (bytesWritten != record.getContentLength())
                log.warn(String.format("Expected %d bytes, but wrote %d", record.getContentLength(), bytesWritten));
        }

        // Write the two blank lines at end of all records
        out.write(CRLF_BYTES);
        out.write(CRLF_BYTES);
    }

    protected static String createRecordHeader(WARCRecordInfo record) {
        final StringBuilder sb = new StringBuilder();

        // WARC record identifier
        sb.append(WARC_ID).append(CRLF);

        // WARC record mandatory headers
        sb.append(HEADER_KEY_ID).append(COLON_SPACE).append('<').append(record.getRecordId().toString()).append('>').append(CRLF);
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
            for (final Iterator<Element> i = record.getExtraHeaders().iterator(); i.hasNext();) {
                sb.append(i.next()).append(CRLF);
            }
        }

        return sb.toString();
    }
}
