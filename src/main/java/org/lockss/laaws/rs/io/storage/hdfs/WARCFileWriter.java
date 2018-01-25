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

package org.lockss.laaws.rs.io.storage.hdfs;

// Dependencies on webarchive-commons
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.archive.format.warc.WARCConstants;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.util.ArchiveUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.archive.util.anvl.Element;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.event.FileWrittenEvent;
import org.springframework.data.hadoop.store.event.StoreEventPublisher;
import org.springframework.data.hadoop.store.output.AbstractDataStreamWriter;
import org.springframework.data.hadoop.store.support.OutputContext;
import org.springframework.data.hadoop.store.support.StreamsHolder;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.UUID;

public class WARCFileWriter extends AbstractDataStreamWriter implements DataStoreWriter<WARCRecordInfo>, WARCConstants {
    private final static Log log = LogFactory.getLog(WARCFileWriter.class);

    // These constants should be moved elsewhere
    private static final String SCHEME = "urn:uuid";
    private static final String SCHEME_COLON = SCHEME + ":";
    private static final byte CR = 13;
    private static final byte LF = 10;
    private static final byte COLON = 58;
    private static final byte SP = 32;
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final String CRLF = "\r\n";
    private static byte[] CRLF_BYTES;
    static {
        try {
            CRLF_BYTES = CRLF.getBytes(DEFAULT_ENCODING);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private StreamsHolder<OutputStream> streamsHolder;

    public WARCFileWriter(Configuration configuration, Path basePath, CodecInfo codec) {
        super(configuration, basePath, codec);
    }

    // This is the default implementation pattern
    @Override
    public synchronized void close() throws IOException {
        if (streamsHolder != null) {
            // Set streamsHolder to null prior to throwing; see AutoCloseable#close() comments.
            IOException rethrow = null;

            try {
                // Close the held output stream
                streamsHolder.close();

                // Rename to the final filename, if an in-writing prefix or suffix was used
                Path path = renameFile(streamsHolder.getPath());

                // Broadcast a FileWrittenEvent, if a StoreEventPublisher was set
                StoreEventPublisher storeEventPublisher= getStoreEventPublisher();
                if (storeEventPublisher != null) {
                    storeEventPublisher.publishEvent(new FileWrittenEvent(this, path));
                }
            } catch (IOException e) {
                rethrow = e;
                log.error("Error in close", e);
            } finally {
                streamsHolder = null;
            }

            if (rethrow != null) {
                throw rethrow;
            }
        }
    }

    // This is the default implementation pattern
    @Override
    public synchronized void flush() throws IOException {
        if (streamsHolder != null) {
            OutputStream stream = streamsHolder.getStream();
            stream.flush();
            if ((isAppendable() || isSyncable()) && stream instanceof Syncable) {
                ((Syncable)stream).hflush();
            }
        }

    }

    // This is the default implementation pattern
    public synchronized void hflush() throws IOException {
        if (streamsHolder != null) {
            ((Syncable)streamsHolder.getStream()).hflush();
        }
    }

    // This is the default implementation pattern
    @Override
    protected void handleTimeout() {
        try {
            if (isAppendable()) {
                log.info("Timeout detected for this writer; flushing stream");
                hflush();
            } else {
                log.info("Timeout detected for this writer; closing stream");
                close();
            }
        } catch (IOException e) {
            log.error("Error closing", e);
        }
        getOutputContext().rollStrategies();
    }

    // Method overridden to automatically write a WARC Info record if we're opening
    // a new stream or overwriting an existing one.
    @Override
    public StreamsHolder<OutputStream> getOutput() throws IOException {
        if (streamsHolder == null) {
            streamsHolder = super.getOutput();

            // Only write the WARC Info record if we're at the beginning of the file
            // and not appending. (If we're appending to a WARC stream, by definition
            // it already had a WARC Info record written to it).
            if (!isAppendable() && getPosition(streamsHolder) == 0) {
                log.info("Writing a WARC Info record");
                /*
                HttpHeaders headers = createWARCInfoRecord();
                writeRecord(streamsHolder.getStream(), headers, "PLACEHOLDER".getBytes());
                */
                write(createWARCInfoRecord(), streamsHolder.getStream());
            }
        }

        return streamsHolder;
    }

    // From webarchive-commons
    public WARCRecordInfo createWARCInfoRecord() throws IOException {
        WARCRecordInfo record = new WARCRecordInfo();
        record.setType(WARCRecordType.warcinfo);
        record.setCreate14DigitDate(ArchiveUtils.get14DigitDate());
        record.setMimetype("application/warc-fields");
        record.addExtraHeader(HEADER_KEY_FILENAME, streamsHolder.getPath().getName());
        record.setRecordId(generateRecordId());

        byte[] contents = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write("Not implemented".getBytes(UTF8));
        contents = baos.toByteArray();
        record.setContentStream(new ByteArrayInputStream(contents));
        record.setContentLength((long)contents.length);

        /*
        HttpHeaders headers = new HttpHeaders();
        headers.add(HEADER_KEY_TYPE, WARCRecordType.warcinfo.name());
        headers.add(HEADER_KEY_DATE, DateUtils.getLog14Date());
        headers.add(HEADER_KEY_ID, makeRecordId());
        headers.add(HEADER_KEY_FILENAME, streamsHolder.getPath().getName());
        headers.add(CONTENT_TYPE, WARC_FIELDS_TYPE);
        */

        return record;
    }

    private static URI generateRecordId() {
        URI uri;

        try {
            uri = new URI(SCHEME_COLON + UUID.randomUUID().toString());
        } catch (URISyntaxException e) {
            // This should never happen
            throw new RuntimeException(e);
        }

        return uri;
    }

    // From webarchive-commons
    private static String makeRecordId() {
        StringBuilder id = new StringBuilder();
        id.append("<").append(SCHEME_COLON);
        id.append(UUID.randomUUID().toString());
        id.append(">");
        return id.toString();
    }

    @Override
    public synchronized void write(WARCRecordInfo record) throws IOException {
        if (streamsHolder == null) {
            streamsHolder = getOutput();
        }

        record.setWARCFilename(streamsHolder.getPath().getName());
        record.setWARCFileOffset(getPosition(streamsHolder));

        OutputStream out = streamsHolder.getStream();

        // Write the WARC record
        write(record, out);

        // Update the writer's position with the position of the output stream
        setWritePosition(getPosition(streamsHolder));

        // Always flush record to HDFS when finished
        hflush();

        // Check whether it's time to roll strategies
        OutputContext context = getOutputContext();
        if (context.getRolloverState()) {
            log.info("Rollover conditions were met; closing current stream");
            close();
            context.rollStrategies();
        }
    }


    /*
    // From webarchive-commons with some modifications
    private void writeRecord(OutputStream out, HttpHeaders headers, byte[] contents) throws IOException {
        // Add Content-Length header
        if (contents == null) {
            headers.add(CONTENT_LENGTH, "0");
        } else {
            headers.add(CONTENT_LENGTH, String.valueOf(contents.length));
        }

        out.write(WARC_ID.getBytes(DEFAULT_ENCODING));

        out.write(CRLF_BYTES);

        // Write headers bytes
        writeRecordHeaders(out, headers);

        out.write(CRLF_BYTES);

        if (contents != null) {
            out.write(contents);
        }

        // Emit the 2 trailing CRLF sequences.
        out.write(CRLF_BYTES);
        out.write(CRLF_BYTES);
    }

    private void writeRecordHeaders(OutputStream out, HttpHeaders headers) throws IOException {
        Iterator i = headers.entrySet().iterator();

        while (i.hasNext()) {
           Map.Entry<String, List<String>> entry = (Map.Entry)i.next();
           for (String v: entry.getValue()) {
               out.write(entry.getKey().getBytes(UTF8));
               out.write(COLON);
               out.write(SP);
               out.write(v.getBytes(UTF8));
               out.write(CRLF_BYTES);
           }
        }
    }



    private HttpHeaders createWARCRecordHeaders(WARCRecordInfo record) {
        // Construct WARC record headers
        HttpHeaders headers = new HttpHeaders();

        // Mandatory WARC record headers
        headers.add(HEADER_KEY_ID, record.getRecordId().toString());
        headers.add(HEADER_KEY_TYPE, record.getType().toString());
        headers.add(HEADER_KEY_DATE, record.getCreate14DigitDate());

        // Optional fields
        headers.add("Content-Length", String.valueOf(record.getContentLength()));
        headers.add(HEADER_KEY_BLOCK_DIGEST, "");
        headers.add(HEADER_KEY_PAYLOAD_DIGEST, "");

        return headers;
    }
    */

    // Experimental: Exposed for use in calculating
    public long getPosition() throws IOException {
        return getPosition(getOutput());
    }

    public static void write(WARCRecordInfo record, OutputStream out) throws IOException {
        if (record.getContentLength() == 0 &&
                (record.getExtraHeaders() == null || record.getExtraHeaders().size() <= 0)) {
            throw new IllegalArgumentException("Cannot write record of content-length zero and base headers only");
        }

        // Write out the header
        out.write(createRecordHeader(record).getBytes(WARC_HEADER_ENCODING));

        // Write out the header/body separator
        out.write(CRLF_BYTES);

        if (record.getContentStream() != null && record.getContentLength() > 0) {
            //contentBytes += copyFrom(recordInfo.getContentStream(), recordInfo.getContentLength(), recordInfo.getEnforceLength());
            IOUtils.copy(record.getContentStream(), out);
        }

        // Write out the two blank lines at end of all records.
        out.write(CRLF_BYTES);
        out.write(CRLF_BYTES);
    }

    protected static String createRecordHeader(WARCRecordInfo record) {
        final StringBuilder sb = new StringBuilder(2048/*A SWAG: TODO: Do analysis.*/);

        sb.append(WARC_ID).append(CRLF);
        sb.append(HEADER_KEY_TYPE).append(COLON_SPACE).append(record.getType()).append(CRLF);

        // Do not write a subject-uri if not one present.
        if (!StringUtils.isEmpty(record.getUrl())) {
            sb.append(HEADER_KEY_URI).append(COLON_SPACE).
                    //append(checkHeaderValue(metaRecord.getUrl())).append(CRLF);
            append(record.getUrl()).append(CRLF);
        }

        sb.append(HEADER_KEY_DATE).append(COLON_SPACE).append(record.getCreate14DigitDate()).append(CRLF);

        if (record.getExtraHeaders() != null) {
            for (final Iterator<Element> i = record.getExtraHeaders().iterator(); i.hasNext();) {
                sb.append(i.next()).append(CRLF);
            }
        }

        sb.append(HEADER_KEY_ID).append(COLON_SPACE).append('<').append(record.getRecordId().toString()).append('>').append(CRLF);

        if (record.getContentLength() > 0) {
            sb.append(CONTENT_TYPE).append(COLON_SPACE).append(
                    //checkHeaderLineMimetypeParameter(metaRecord.getMimetype())).append(CRLF);
                    record.getMimetype()).append(CRLF);
        }

        sb.append(CONTENT_LENGTH).append(COLON_SPACE).append(Long.toString(record.getContentLength())).append(CRLF);

        return sb.toString();
    }
}
