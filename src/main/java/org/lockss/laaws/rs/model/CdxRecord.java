/*

Copyright (c) 2000-2019 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */
package org.lockss.laaws.rs.model;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import org.lockss.log.L4JLogger;

/**
 * An [Open|Py]Wayback CDX record.
 */
public class CdxRecord {
  private static L4JLogger log = L4JLogger.getLogger();

  private static DateTimeFormatter dateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

  private String urlSortKey;
  private long timestamp;
  private String url;
  private String mimeType;
  private int httpStatus;
  private String digest;
  private String redirectUrl = "-";
  private String robotFlags = "-";
  private long length;
  private long offset;
  private String archiveName;

  /**
   * Provides the URL sort key.
   * 
   * @return a String with the URL sort key.
   */
  public String getUrlSortKey() {
    return urlSortKey;
  }

  /**
   * Saves the URL sort key.
   * 
   * @param urlSortKey
   *          A String with the URL sort key.
   */
  public void setUrlSortKey(String urlSortKey) {
    this.urlSortKey = urlSortKey;
  }

  /**
   * Provides the timestamp.
   * 
   * @return a long with the timestamp.
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Saves the timestamp.
   * 
   * @param timestamp A long with the timestamp.
   */
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * Provides the URL.
   * 
   * @return a String with the URL.
   */
  public String getUrl() {
    return url;
  }

  /**
   * Saves the URL.
   * 
   * @param url
   *          A String with the URL.
   */
  public void setUrl(String url) {
    this.url = url;
  }

  /**
   * Provides the MIME type.
   * 
   * @return a String with the MIME type.
   */
  public String getMimeType() {
    return mimeType;
  }

  /**
   * Saves the MIME type.
   * 
   * @param mimeType
   *          A String with the MIME type.
   */
  public void setMimeType(String mimeType) {
    this.mimeType = mimeType;
  }

  /**
   * Provides the HTTP status code.
   * 
   * @return an int with the HTTP status code.
   */
  public int getHttpStatus() {
    return httpStatus;
  }

  /**
   * Saves the HTTP status code.
   * 
   * @param httpStatus
   *          An int with the HTTP status code.
   */
  public void setHttpStatus(int httpStatus) {
    this.httpStatus = httpStatus;
  }

  /**
   * Provides the digest.
   * 
   * @return a String with the digest.
   */
  public String getDigest() {
    return digest;
  }

  /**
   * Saves the digest.
   * 
   * @param digest
   *          A String with the digest.
   */
  public void setDigest(String digest) {
    this.digest = digest;
  }

  /**
   * Provides the redirect URL.
   * 
   * @return a String with the redirect URL.
   */
  public String getRedirectUrl() {
    return redirectUrl;
  }

  /**
   * Saves the redirect URL.
   * 
   * @param redirectUrl
   *          A String with the redirect URL.
   */
  public void setRedirectUrl(String redirectUrl) {
    this.redirectUrl = redirectUrl;
  }

  /**
   * Provides the robot flags.
   * 
   * @return a String with the robot flags.
   */
  public String getRobotFlags() {
    return robotFlags;
  }

  /**
   * Saves the robot flags.
   * 
   * @param robotFlags
   *          A String with the robot flags.
   */
  public void setRobotFlags(String robotFlags) {
    this.robotFlags = robotFlags;
  }

  /**
   * Provides the content length.
   * 
   * @return a long with the content length.
   */
  public long getLength() {
    return length;
  }

  /**
   * Saves the content length.
   * 
   * @param length
   *          A long with the content length.
   */
  public void setLength(long length) {
    this.length = length;
  }

  /**
   * Provides the content offset.
   * 
   * @return a long with the content offset.
   */
  public long getOffset() {
    return offset;
  }

  /**
   * Saves the content offset.
   * 
   * @param offset
   *          A long with the content offset.
   */
  public void setOffset(long offset) {
    this.offset = offset;
  }

  /**
   * Provides the archive name.
   * 
   * @return a String with the archive name.
   */
  public String getArchiveName() {
    return archiveName;
  }

  /**
   * Saves the archive name.
   * 
   * @param archiveName
   *          A long with the archive name.
   */
  public void setArchiveName(String archiveName) {
    this.archiveName = archiveName;
  }

  /**
   * Provides the representation of this object in Internet Archive format.
   * 
   * @return a String with the representation of this object in Internet Archive
   *         format.
   */
  public String toIaText() {
    log.debug2("Invoked.");

    StringBuilder out = new StringBuilder();

    out.append(urlSortKey).append(' ');
    out.append(timestamp).append(' ');
    out.append(url).append(' ');
    out.append(mimeType).append(' ');
    out.append(httpStatus).append(' ');
    out.append(digest).append(' ');
    out.append(redirectUrl).append(' ');
    out.append(robotFlags).append(' ');
    out.append(length).append(' ');
    out.append(offset).append(' ');
    out.append(archiveName).append("\n");

    String result = out.toString();
    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Writes the representation of this object in OpenWayback XML format.
   * 
   * @param writer
   *          An XMLStreamWriter where to output the XML representation of this
   *          object.
   * @exception XMLStreamException
   *              if there are problems writing the XML element.
   */
  public void toXmlText(XMLStreamWriter writer) throws XMLStreamException {
    log.debug2("Invoked.");

    try {
      // Start the result element.
      writer.writeStartElement("result");

      // Add all the results sub-elements.
      writeXmlElement(writer, "compressedoffset", offset);
      writeXmlElement(writer, "compressedendoffset", length);
      writeXmlElement(writer, "mimetype", mimeType);
      writeXmlElement(writer, "file", archiveName);
      writeXmlElement(writer, "redirecturl", redirectUrl);
      writeXmlElement(writer, "urlkey", urlSortKey);
      writeXmlElement(writer, "digest", digest);
      writeXmlElement(writer, "httpresponsecode", httpStatus);
      writeXmlElement(writer, "robotflags", robotFlags);
      writeXmlElement(writer, "url", url);
      writeXmlElement(writer, "capturedate", timestamp);

      // Finish the result element.
      writer.writeEndElement();
    } catch (XMLStreamException xse) {
      log.error("Exception caught writing XML", xse);
      throw xse;
    }

    log.debug2("Done.");
  }

  /**
   * Utility method to output an XML element.
   * 
   * @param writer
   *          An XMLStreamWriter where to output the XML element.
   * @param name
   *          A String with the name of the element.
   * @param value
   *          An Object with the value of the element.
   * @exception XMLStreamException
   *              if there are problems writing the XML element.
   */
  static void writeXmlElement(XMLStreamWriter writer, String name, Object value)
      throws XMLStreamException {
    log.debug2("name = {}", name);
    log.debug2("value = {}", value);

    try {
      writer.writeStartElement(name);
      writer.writeCharacters(value.toString());
      writer.writeEndElement();
    } catch (XMLStreamException xse) {
      log.error("Exception caught writing XML for element:"
	  + " name = {}, value = {}", name, value, xse);
      throw xse;
    }

    log.debug2("Done.");
  }

  /**
   * Provides the representation of this object in JSON format.
   * 
   * @return a String with the representation of this object in JSON format.
   */
  public String toJson() {
    log.debug2("Invoked.");

    StringBuilder out = new StringBuilder();

    out.append(urlSortKey).append(' ');
    out.append(timestamp).append(' ');
    out.append("{");
    out.append("\"url\": \"").append(url).append("\", ");
    out.append("\"mime\": \"").append(mimeType).append("\", ");
    out.append("\"status\": \"").append(httpStatus).append("\", ");
    out.append("\"digest\": \"").append(digest).append("\", ");
    out.append("\"length\": \"").append(length).append("\", ");
    out.append("\"offset\": \"").append(offset).append("\", ");
    out.append("\"filename\": \"").append(archiveName).append("\"");
    out.append("}\n");

    String result = out.toString();
    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Provides the artifact collection date that corresponds to a given text
   * timestamp.
   * 
   * @param timestamp
   *          A String with the timestamp.
   * @return a long with the artifact collection date.
   */
  public static long computeCollectiondate(String timestamp) {
    return LocalDateTime.parse(timestamp, dateTimeFormatter)
	.toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  /**
   * Provides the text timestamp that corresponds to a given artifact collection
   * date.
   * 
   * @param collectionDate
   *          A long with the artifact collection date.
   * @return a String with the timestamp.
   */
  public static String computeTextTimestamp(long collectionDate) {
    return dateTimeFormatter.format(LocalDateTime.ofEpochSecond(
	collectionDate / 1000, 0, ZoneOffset.UTC));
  }

  /**
   * Provides the numeric timestamp that corresponds to a given artifact
   * collection date.
   * 
   * @param collectionDate
   *          A long with the artifact collection date.
   * @return a long with the numeric timestamp.
   */
  public static long computeNumericTimestamp(long collectionDate) {
    return Long.parseLong(computeTextTimestamp(collectionDate));
  }

  @Override
  public String toString() {
    return "[CdxRecord urlSortKey=" + urlSortKey + ", timestamp=" + timestamp
	+ ", url=" + url + ", mimeType=" + mimeType + ", httpStatus="
	+ httpStatus + ", digest=" + digest + ", redirectUrl=" + redirectUrl
	+ ", robotFlags=" + robotFlags + ", length=" + length + ", offset="
	+ offset + ", archiveName=" + archiveName + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CdxRecord cdxRecord = (CdxRecord) o;
    return Objects.equals(urlSortKey, cdxRecord.urlSortKey) &&
        Objects.equals(timestamp, cdxRecord.timestamp) &&
        Objects.equals(url, cdxRecord.url) &&
        Objects.equals(mimeType, cdxRecord.mimeType) &&
        Objects.equals(httpStatus, cdxRecord.httpStatus) &&
        Objects.equals(digest, cdxRecord.digest) &&
        Objects.equals(redirectUrl, cdxRecord.redirectUrl) &&
        Objects.equals(robotFlags, cdxRecord.robotFlags) &&
        Objects.equals(length, cdxRecord.length) &&
        Objects.equals(offset, cdxRecord.offset) &&
        Objects.equals(archiveName, cdxRecord.archiveName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(urlSortKey, timestamp, url, mimeType, httpStatus,
	digest, redirectUrl, robotFlags, length, offset, archiveName);
  }
}
