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
import org.archive.format.warc.WARCConstants;
import org.archive.io.warc.WARCRecordInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.lockss.laaws.rs.io.storage.WarcArtifactStore;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.codec.CodecInfo;
import org.springframework.data.hadoop.store.event.FileWrittenEvent;
import org.springframework.data.hadoop.store.event.StoreEventPublisher;
import org.springframework.data.hadoop.store.output.AbstractDataStreamWriter;
import org.springframework.data.hadoop.store.support.OutputContext;
import org.springframework.data.hadoop.store.support.StreamsHolder;

import java.io.*;
import java.nio.charset.Charset;

public class HdfsWarcFileWriter extends AbstractDataStreamWriter implements DataStoreWriter<WARCRecordInfo>, WARCConstants {
    private final static Log log = LogFactory.getLog(HdfsWarcFileWriter.class);

    private StreamsHolder<OutputStream> streamsHolder;

    public HdfsWarcFileWriter(Configuration configuration, Path basePath, CodecInfo codec) {
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
                WarcArtifactStore.writeWarcRecord(
                        WarcArtifactStore.createWARCInfoRecord(null),
                        streamsHolder.getStream()
                );
            }
        }

        return streamsHolder;
    }

    // Experimental: Exposed for use in calculating
    public long getPosition() throws IOException {
        return getPosition(getOutput());
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
        WarcArtifactStore.writeWarcRecord(record, out);

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


//    // From webarchive-commons with some modifications
//    private void writeRecord(OutputStream out, HttpHeaders headers, byte[] contents) throws IOException {
//        // Add Content-Length header
//        if (contents == null) {
//            headers.add(CONTENT_LENGTH, "0");
//        } else {
//            headers.add(CONTENT_LENGTH, String.valueOf(contents.length));
//        }
//
//        out.write(WARC_ID.getBytes(DEFAULT_ENCODING));
//
//        out.write(CRLF_BYTES);
//
//        // Write headers bytes
//        writeRecordHeaders(out, headers);
//
//        out.write(CRLF_BYTES);
//
//        if (contents != null) {
//            out.write(contents);
//        }
//
//        // Emit the 2 trailing CRLF sequences.
//        out.write(CRLF_BYTES);
//        out.write(CRLF_BYTES);
//    }
//
//    private void writeRecordHeaders(OutputStream out, HttpHeaders headers) throws IOException {
//        Iterator i = headers.entrySet().iterator();
//
//        while (i.hasNext()) {
//           Map.Entry<String, List<String>> entry = (Map.Entry)i.next();
//           for (String v: entry.getValue()) {
//               out.write(entry.getKey().getBytes(UTF8));
//               out.write(COLON);
//               out.write(SP);
//               out.write(v.getBytes(UTF8));
//               out.write(CRLF_BYTES);
//           }
//        }
//    }
//
//    private HttpHeaders createWARCRecordHeaders(WARCRecordInfo record) {
//        // Construct WARC record headers
//        HttpHeaders headers = new HttpHeaders();
//
//        // Mandatory WARC record headers
//        headers.add(HEADER_KEY_ID, record.getRecordId().toString());
//        headers.add(HEADER_KEY_TYPE, record.getType().toString());
//        headers.add(HEADER_KEY_DATE, record.getCreate14DigitDate());
//
//        // Optional fields
//        headers.add("Content-Length", String.valueOf(record.getContentLength()));
//        headers.add(HEADER_KEY_BLOCK_DIGEST, "");
//        headers.add(HEADER_KEY_PAYLOAD_DIGEST, "");
//
//        return headers;
//    }
}
