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

import java.io.StringWriter;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import org.lockss.log.L4JLogger;

/**
 * A collection of [Open|Py]Wayback CDX records.
 */
public class CdxRecords {
  private static L4JLogger log = L4JLogger.getLogger();

  // The CDX records in this object.
  private List<CdxRecord> cdxRecords = new ArrayList<>();

  // The elements of the OpenWayBack query that results in this collection of
  // CDX records.
  private Map<String, String> openWayBackQuery = null;

  // The character set name to be used for XML generation.
  private String charsetName = null;

  /**
   * No-argument constructor to be used when processing PyWayBack requests.
   */
  public CdxRecords() {
  }

  /**
   * Constructor to be used when processing OpenWayBack requests.
   * 
   * @param openWayBackQuery
   *          A Map<String, String> with the OpenWayBack request query elements.
   * @param charsetName
   *          A String with the character set name to be used for XML
   *          generation.
   */
  public CdxRecords(Map<String, String> openWayBackQuery, String charsetName) {
    this.openWayBackQuery = openWayBackQuery;
    this.charsetName = charsetName;
  }

  /**
   * Adds a CDX record to this object.
   * 
   * @param cdxRecord
   *          A CdxRecord with the CDX record to be added.
   */
  public void addCdxRecord(CdxRecord cdxRecord) {
    cdxRecords.add(cdxRecord);
  }

  /**
   * Provides the count of CDX records in this object.
   * 
   * @return an int with the count of CDX records in this object.
   */
  public int getCdxRecordCount() {
    return cdxRecords.size();
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

    for (CdxRecord cdxRecord : cdxRecords) {
      out.append(cdxRecord.toIaText()).append("\n");
    }

    String result = out.toString();
    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Writes the representation of this object in OpenWayback XML format.
   * 
   * @return a String with the representation of this object in OpenWayBack XML
   *         format.
   * @exception XMLStreamException
   *              if there are problems writing the XML element.
   */
  public String toXmlText() throws XMLStreamException {
    StringWriter sw = new StringWriter();

    try {
      XMLStreamWriter writer =
	  XMLOutputFactory.newInstance().createXMLStreamWriter(sw);

      writer.writeStartDocument(charsetName, "1.0");

      // Start the top element.
      writer.writeStartElement("wayback");

      // Start the request element.
      writer.writeStartElement("request");

      // Add all the request sub-elements.
      CdxRecord.writeXmlElement(writer, "startdate", "19960101000000");

      CdxRecord.writeXmlElement(writer, "enddate",
	  DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
	  .format(LocalDateTime.now(ZoneOffset.UTC)));

      CdxRecord.writeXmlElement(writer, "type",
	  openWayBackQuery.getOrDefault("type", "urlquery").toLowerCase());

      CdxRecord.writeXmlElement(writer, "firstreturned",
	  Long.parseLong(openWayBackQuery.getOrDefault("offset", "0")));

      CdxRecord.writeXmlElement(writer, "url",
	  openWayBackQuery.get("canonicalUrl"));

      CdxRecord.writeXmlElement(writer, "resultsrequested",
	  Long.parseLong(openWayBackQuery.getOrDefault("limit", "10000")));

      CdxRecord.writeXmlElement(writer, "resultstype", "resultstypecapture");

      // Finish the request element.
      writer.writeEndElement();

      // Start the results element.
      writer.writeStartElement("results");

      // Loop through all the results.
      for (CdxRecord cdxRecord : cdxRecords) {
	// Output this result.
	cdxRecord.toXmlText(writer);
      }

      // Finish the results element.
      writer.writeEndElement();

      // Finish the top element.
      writer.writeEndDocument();

      // Cleanup and finish.
      writer.flush();
      sw.flush();
      writer.close();

      // Return the text representation of this XML document.
      String result = sw.toString();
      log.debug2("result = {}", result);
      return result;
    } catch (XMLStreamException xse) {
      log.error("Exception caught writing XML", xse);
      throw xse;
    } finally {
      try {
	sw.close();
      } catch (Exception e) {}
    }
  }
}
