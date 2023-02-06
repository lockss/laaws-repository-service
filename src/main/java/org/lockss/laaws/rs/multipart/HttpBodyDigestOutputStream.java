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

import org.apache.commons.io.output.ProxyOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HttpBodyDigestOutputStream extends ProxyOutputStream {
  private int currentState;
  private DigestOutputStream dos;

  public HttpBodyDigestOutputStream(OutputStream proxy) {
    super(proxy);
  }

  private boolean advanceState(int b) {
    switch (currentState) {
      case 0:
        if (b == '\r')
          currentState = 1;
        break;

      case 1:
        if (b == '\n') {
          currentState = 2;
        } else {
          currentState = 0;
        }
        break;

      case 2:
        if (b == '\r') {
          currentState = 3;
        } else {
          currentState = 0;
        }
        break;

      case 3:
        if (b == '\n') {
          return true;
        } else {
          currentState = 0;
        }
        break;
    }

    return false;
  }

  public static final String DEFAULT_DIGEST_ALGORITHM = "SHA-256";

  private boolean isDigesting() {
    return dos != null;
  }

  public void switchToDigest() {
    try {
      // Create new DigestOutputStream
      dos = new DigestOutputStream(this.out, MessageDigest.getInstance(DEFAULT_DIGEST_ALGORITHM));

    } catch (NoSuchAlgorithmException e) {
      // This should never happen
      // TODO
    }
  }

  /**
   * 1. Write bytes up to and including i to ProxyOutputStream (it is part of the header)
   * 2. Anything left in the buffer goes to the DOS
   */
  private void switchToDigest(byte[] b, int off, int i, int len) throws IOException {
    int next = i + 1;
    super.write(b, off, next-off);
    switchToDigest();
    dos.write(b, next, len-(next-off));
  }

  @Override
  public void write(int b) throws IOException {
    if (isDigesting()) {
      dos.write(b);
    } else {
      super.write(b);
      if (advanceState(b)) {
        // Switch to digest mode
        switchToDigest();
      }
    }
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (isDigesting()) {
      dos.write(b, off, len);
    } else {

      for (int i = off; i < off+len; i++) {
        if (advanceState(b[i])) {
          switchToDigest(b, off, i, len);
          return;
        }
      }

      super.write(b, off, len);
    }
  }

  @Override
  public void flush() throws IOException {
    if (dos != null) dos.flush();
    super.flush();
  }

  @Override
  public void close() throws IOException {
    if (dos != null) dos.close();
    super.close();
  }

  public MessageDigest getDigest() {
    return dos == null ? null : dos.getMessageDigest();
  }
}
