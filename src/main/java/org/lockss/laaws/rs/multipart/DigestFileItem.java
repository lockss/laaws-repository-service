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

import org.apache.commons.fileupload.disk.DiskFileItem;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class DigestFileItem extends DiskFileItem {
  HttpBodyDigestOutputStream output;

  /**
   * The temporary file to use.
   */
  private transient File tempFile;
  private final File repository;

  public DigestFileItem(String fieldName, String contentType, boolean isFormField, String fileName, int sizeThreshold, File repository) {
    super(fieldName, contentType, isFormField, fileName, sizeThreshold, repository);
    this.repository = repository;
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    output = new HttpBodyDigestOutputStream(super.getOutputStream());

    // FIXME: There is a pretty severe problem here with the digest when asHttpResponse=false
    //  but Content-Type is application/http
    if (!getContentType().startsWith("application/http")) {
      output.switchToDigest();
    }

    return output;
  }

  public MessageDigest getDigest() {
    return output.getDigest();
  }

  /**
   * UID used in unique file name generation.
   */
  private static final String UID =
      UUID.randomUUID().toString().replace('-', '_');

  /**
   * Creates and returns a {@link java.io.File File} representing a uniquely
   * named temporary file in the configured repository path. The lifetime of
   * the file is tied to the lifetime of the <code>FileItem</code> instance;
   * the file will be deleted when the instance is garbage collected.
   * <p>
   * <b>Note: Subclasses that override this method must ensure that they return the
   * same File each time.</b>
   *
   * @return The {@link java.io.File File} to be used for temporary storage.
   */
  protected File getTempFile() {
    if (tempFile == null) {
      File tempDir = repository;
      if (tempDir == null) {
        tempDir = new File(System.getProperty("java.io.tmpdir"));
      }

      String tempFileName = String.format("upload_%s_%s.tmp", UID, getUniqueId());

      tempFile = new File(tempDir, tempFileName);
    }
    return tempFile;
  }

  /**
   * Counter used in unique identifier generation.
   */
  private static final AtomicInteger COUNTER = new AtomicInteger(0);

  /**
   * Returns an identifier that is unique within the class loader used to
   * load this class, but does not have random-like appearance.
   *
   * @return A String with the non-random looking instance identifier.
   */
  private static String getUniqueId() {
    final int limit = 100000000;
    int current = COUNTER.getAndIncrement();
    String id = Integer.toString(current);

    // If you manage to get more than 100 million of ids, you'll
    // start getting ids longer than 8 characters.
    if (current < limit) {
      id = ("00000000" + id).substring(id.length());
    }
    return id;
  }
}
