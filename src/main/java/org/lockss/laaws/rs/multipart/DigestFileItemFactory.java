/*

Copyright (c) 2000-2022, Board of Trustees of Leland Stanford Jr. University

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice,
this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/

package org.lockss.laaws.rs.multipart;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.io.FileCleaningTracker;

import java.io.File;

public class DigestFileItemFactory extends DiskFileItemFactory {
  /**
   * Constructs an unconfigured instance of this class. The resulting factory
   * may be configured by calling the appropriate setter methods.
   */
  public DigestFileItemFactory() {
    this(DEFAULT_SIZE_THRESHOLD, null);
  }

  /**
   * Constructs a preconfigured instance of this class.
   *
   * @param sizeThreshold The threshold, in bytes, below which items will be
   *                      retained in memory and above which they will be
   *                      stored as a file.
   * @param repository    The data repository, which is the directory in
   *                      which files will be created, should the item size
   *                      exceed the threshold.
   */
  public DigestFileItemFactory(int sizeThreshold, File repository) {
    super(sizeThreshold, repository);
  }

  @Override
  public FileItem createItem(String fieldName, String contentType,
                             boolean isFormField, String fileName) {

    DigestFileItem result = new DigestFileItem(fieldName, contentType,
        isFormField, fileName, getSizeThreshold(), getRepository());

    result.setDefaultCharset(getDefaultCharset());

    FileCleaningTracker tracker = getFileCleaningTracker();

    if (tracker != null) {
      tracker.track(result.getTempFile(), result);
    }

    return result;
  }
}
