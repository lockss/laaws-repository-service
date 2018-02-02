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

import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.RepositoryArtifactMetadata;

import java.io.File;

/**
 * Extends the RepositoryArtifactMetadata class to additionally track an artifact's WARC record location (i.e., the pair
 * of "WARC file path" and "offset to beginning of WARC record").
 *
 */
public class WarcRepositoryArtifactMetadata extends RepositoryArtifactMetadata {
    public static String WARCFILE_PATH_KEY = "warcFile";
    public static String WARCFILE_OFFSET_KEY = "warcRecordOffset";

//    public static String LOCKSS_METADATA_ID = "lockss-repo";

//    @Override
//    public String getMetadataId() {
//        return this.LOCKSS_METADATA_ID;
//    }

    public WarcRepositoryArtifactMetadata(ArtifactIdentifier identifier, String warcFilePath, long offset, boolean committed, boolean deleted) {
        super(identifier);
        this.setWarcFilePath(warcFilePath);
        this.setWarcRecordOffset(offset);
        this.setCommitted(committed);
        this.setDeleted(deleted);
    }

    public String getWarcFilePath() {
        return this.getString(WARCFILE_PATH_KEY);
    }

    public long getWarcRecordOffset() {
        return this.getLong(WARCFILE_OFFSET_KEY);
    }

    public void setWarcFilePath(String warcFilePath) {
        this.put(WARCFILE_PATH_KEY, warcFilePath);
    }

    public void setWarcRecordOffset(long warcRecordOffset) {
        this.put(WARCFILE_OFFSET_KEY, warcRecordOffset);
    }
}
