/*

 Copyright (c) 2019 Board of Trustees of Leland Stanford Jr. University,
 all rights reserved.

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 STANFORD UNIVERSITY BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
 IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 Except as contained in this notice, the name of Stanford University shall not
 be used in advertising or otherwise to promote the sale, use or other dealings
 in this Software without prior written authorization from Stanford University.

 */
package org.lockss.laaws.rs.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicStatusLine;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.core.VolatileLockssRepository;
import org.lockss.laaws.rs.impl.CdxApiServiceImpl.ClosestArtifact;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.CdxRecord;
import org.lockss.laaws.rs.model.CdxRecords;
import org.lockss.test.SpringLockssTestCase;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

/**
 * Test class for org.lockss.laaws.rs.impl.CdxApiServiceImpl.
 */
public class TestCdxApiServiceImpl extends SpringLockssTestCase {
  private LockssRepository repository;

  /**
   * Sets up a test repository.
   * 
   * @throws Exception
   *           if there are problems creating the repository.
   */
  @Before
  public void setUpArtifactDataStore() throws Exception {
    repository = new VolatileLockssRepository();
    repository.initRepository();
  }

  /**
   * Tears down the repository after each test.
   */
  @After
  public void tearDownArtifactDataStore() {
      this.repository = null;
  }

  /**
   * Tests the repository readiness.
   */
  @Test
  public void testRepository() {
    assertNotNull(repository);
    assertTrue(repository.isReady());
  }

  /**
   * Tests the ClosestArtifact inner class.
   * @exception IOException
   *              if there are problems creating the CDX record.
   */
  @Test
  public void testClosestArtifact() throws IOException {
    long collectiondate = 0;

    Artifact art1 = makeArtifact("coll1", "auid1", "url1", 1,
	MediaType.TEXT_HTML, collectiondate);

    long targetTimestamp = 12345;

    ClosestArtifact ca = new ClosestArtifact(art1, targetTimestamp);
    assertEquals(art1, ca.getArtifact());
    assertEquals(targetTimestamp, ca.getGap());

    collectiondate = 12345;
    art1.setCollectionDate(collectiondate);

    assertEquals(0, new ClosestArtifact(art1, collectiondate).getGap());
    assertEquals(100, new ClosestArtifact(art1, collectiondate - 100).getGap());
    assertEquals(100, new ClosestArtifact(art1, collectiondate + 100).getGap());
  }

  /**
   * Tests the creation of a CDX record.
   *
   * @exception Exception
   *              if there are problems creating the CDX record.
   */
  @Test
  public void testGetCdxRecord() throws Exception {
    ArtifactData ad = makeArtifactData("id1", "coll1", "auid1",
	"url1.example.com", 1, MediaType.TEXT_HTML, 24 * 60 * 60 * 1000);
    ad.setContentDigest("cd1");
    ad.setContentLength(9876);

    CdxRecord cdxRecord = new CdxApiServiceImpl(null).getCdxRecord(ad);

    String expected = "(com,example,url1,)/ 19700102000000 url1.example.com"
	+ " text/html 200 cd1 - - 9876 0 coll1:id1.warc\n";
    assertEquals(expected, cdxRecord.toIaText());

    expected = "(com,example,url1,)/ 19700102000000 {\"url\":"
	+ " \"url1.example.com\", \"mime\": \"text/html\", \"status\": \"200\","
	+ " \"digest\": \"cd1\", \"length\": \"9876\", \"offset\": \"0\","
	+ " \"filename\": \"coll1:id1.warc\"}\n";
    assertEquals(expected, cdxRecord.toJson());

    StringWriter sw = new StringWriter();
    XMLStreamWriter writer =
	XMLOutputFactory.newInstance().createXMLStreamWriter(sw);

    cdxRecord.toXmlText(writer);
    writer.flush();
    sw.flush();
    writer.close();

    expected = "<result><compressedoffset>0</compressedoffset>"
	+ "<compressedendoffset>9876</compressedendoffset>"
	+ "<mimetype>text/html</mimetype><file>coll1:id1.warc</file>"
	+ "<redirecturl>-</redirecturl><urlkey>(com,example,url1,)/</urlkey>"
	+ "<digest>cd1</digest><httpresponsecode>200</httpresponsecode>"
	+ "<robotflags>-</robotflags><url>url1.example.com</url>"
	+ "<capturedate>19700102000000</capturedate></result>";
    assertEquals(expected, sw.toString());
  }

  /**
   * Tests the sorting of artifacts by temporal gap.
   *
   * @exception IOException
   *              if there are problems creating the CDX record.
   */
  @Test
  public void testGetArtifactsSortedByTemporalGap() throws IOException {
    List<Artifact> artifacts = new ArrayList<>();

    artifacts.add(makeArtifact("coll1", "auid1", "url1", 1,
	MediaType.TEXT_HTML, 12345));
    artifacts.add(makeArtifact("coll1", "auid1", "url1", 2,
	MediaType.TEXT_HTML, 12345 + 6 * 60 * 60 * 1000));
    artifacts.add(makeArtifact("coll1", "auid1", "url1", 3,
	MediaType.TEXT_HTML, 12345 + 16 * 60 * 60 * 1000));

    // Test closest to date earlier than all artifacts.
    List<Artifact> sortedArtifacts = new CdxApiServiceImpl(null)
	.getArtifactsSortedByTemporalGap(artifacts.iterator(),
	    "19700101000001");

    assertEquals(1, sortedArtifacts.get(0).getVersion().intValue());
    assertEquals(2, sortedArtifacts.get(1).getVersion().intValue());
    assertEquals(3, sortedArtifacts.get(2).getVersion().intValue());

    // Test closest to date immediately after the first artifact.
    sortedArtifacts = new CdxApiServiceImpl(null)
	.getArtifactsSortedByTemporalGap(artifacts.iterator(),
	    "19700101020000");

    assertEquals(1, sortedArtifacts.get(0).getVersion().intValue());
    assertEquals(2, sortedArtifacts.get(1).getVersion().intValue());
    assertEquals(3, sortedArtifacts.get(2).getVersion().intValue());

    // Test closest to date immediately before the second artifact.
    sortedArtifacts = new CdxApiServiceImpl(null)
	.getArtifactsSortedByTemporalGap(artifacts.iterator(),
	    "19700101070000");

    assertEquals(2, sortedArtifacts.get(0).getVersion().intValue());
    assertEquals(1, sortedArtifacts.get(1).getVersion().intValue());
    assertEquals(3, sortedArtifacts.get(2).getVersion().intValue());

    // Test closest to date immediately after the second artifact.
    sortedArtifacts = new CdxApiServiceImpl(null)
	.getArtifactsSortedByTemporalGap(artifacts.iterator(),
	    "19700101090000");

    assertEquals(2, sortedArtifacts.get(0).getVersion().intValue());
    assertEquals(3, sortedArtifacts.get(1).getVersion().intValue());
    assertEquals(1, sortedArtifacts.get(2).getVersion().intValue());

    // Test closest to date immediately before the third artifact.
    sortedArtifacts = new CdxApiServiceImpl(null)
	.getArtifactsSortedByTemporalGap(artifacts.iterator(),
	    "19700101120000");

    assertEquals(3, sortedArtifacts.get(0).getVersion().intValue());
    assertEquals(2, sortedArtifacts.get(1).getVersion().intValue());
    assertEquals(1, sortedArtifacts.get(2).getVersion().intValue());

    // Test closest to date immediately after all artifacts.
    sortedArtifacts = new CdxApiServiceImpl(null)
	.getArtifactsSortedByTemporalGap(artifacts.iterator(),
	    "19700101180000");

    assertEquals(3, sortedArtifacts.get(0).getVersion().intValue());
    assertEquals(2, sortedArtifacts.get(1).getVersion().intValue());
    assertEquals(1, sortedArtifacts.get(2).getVersion().intValue());
  }

  /**
   * Tests the creation of CDX records for a collection of artifacts.
   *
   * @exception Exception
   *              if there are problems creating the CDX records.
   */
  @Test
  public void testGetArtifactsCdxRecords() throws Exception {
    // Populate the repository.
    List<Artifact> artifacts = new ArrayList<>();

    artifacts.add(makeArtifact("coll1", "auid1", "url1.example.com", 1,
	MediaType.TEXT_HTML, 12345));
    artifacts.add(makeArtifact("coll1", "auid1", "url2.example.com", 2,
	MediaType.TEXT_HTML, 23456));
    artifacts.add(makeArtifact("coll1", "auid1", "url3.example.com", 3,
	MediaType.TEXT_HTML, 34567));
    artifacts.add(makeArtifact("coll1", "auid1", "url4.example.com", 1,
	MediaType.TEXT_HTML, 112345));
    artifacts.add(makeArtifact("coll1", "auid1", "url5.example.com", 2,
	MediaType.TEXT_HTML, 123456));
    artifacts.add(makeArtifact("coll1", "auid1", "url6.example.com", 3,
	MediaType.TEXT_HTML, 134567));
    artifacts.add(makeArtifact("coll1", "auid1", "url7.example.com", 1,
	MediaType.TEXT_HTML, 212345));
    artifacts.add(makeArtifact("coll1", "auid1", "url8.example.com", 2,
	MediaType.TEXT_HTML, 223456));
    artifacts.add(makeArtifact("coll1", "auid1", "url9.example.com", 3,
	MediaType.TEXT_HTML, 234567));

    // Get all CDX records.
    CdxRecords records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, null, null, records);

    assertEquals(artifacts.size(), records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    List<CdxRecord> allCdxRecords = records.getCdxRecords();

    // Get sets of CDX records that include all of them.
    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, null, 1, records);
    assertIterableEquals(allCdxRecords, records.getCdxRecords());

    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, null, 2, records);
    assertIterableEquals(allCdxRecords, records.getCdxRecords());

    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 10000, 1, records);
    assertIterableEquals(allCdxRecords, records.getCdxRecords());

    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, artifacts.size(), 1, records);
    assertIterableEquals(allCdxRecords, records.getCdxRecords());

    // Get sets of CDX records with a page size of 8.
    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 8, 1, records);

    assertEquals(8, records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 8, 2, records);

    assertEquals(1, records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i + 8).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    // Get sets of CDX records with a page size of 4.
    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 4, 1, records);

    assertEquals(4, records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 4, 2, records);

    assertEquals(4, records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i + 4).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 4, 3, records);

    assertEquals(1, records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i + 8).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    // Get sets of CDX records with a page size of 3.
    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 3, 1, records);

    assertEquals(3, records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 3, 2, records);

    assertEquals(3, records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i + 3).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 3, 3, records);

    assertEquals(3, records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i + 6).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    // Get sets of CDX records with a page size of 1.
    for (int pageIdx = 0; pageIdx < 9; pageIdx++) {
      records = new CdxRecords();
      new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	  repository, 1, pageIdx + 1, records);

      assertEquals(1, records.getCdxRecordCount());
      assertEquals(artifacts.get(pageIdx).getUri(),
	  records.getCdxRecords().get(0).getUrl());
    }

    // Get empty results beyond the last CDX record.
    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 10000, 2, records);
    assertEquals(0, records.getCdxRecordCount());

    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, artifacts.size(), 2, records);
    assertEquals(0, records.getCdxRecordCount());

    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 8, 3, records);
    assertEquals(0, records.getCdxRecordCount());

    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 4, 4, records);
    assertEquals(0, records.getCdxRecordCount());

    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 3, 4, records);
    assertEquals(0, records.getCdxRecordCount());

    records = new CdxRecords();
    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 1, 10, records);
    assertEquals(0, records.getCdxRecordCount());
  }

  /**
   * Tests the creation of CDX records for URLs.
   *
   * @exception Exception
   *              if there are problems creating the CDX records.
   */
  @Test
  public void testGetCdxRecords() throws Exception {
    // Populate the repository.
    List<Artifact> artifacts = new ArrayList<>();

    artifacts.add(makeArtifact("coll1", "auid1", "www.url4.example.com", 1,
	MediaType.TEXT_HTML, 312345));
    artifacts.add(makeArtifact("coll1", "auid1", "www.url3.example.com", 1,
	MediaType.TEXT_HTML, 212345));
    artifacts.add(makeArtifact("coll1", "auid1", "www.url2.example.com", 3,
	MediaType.TEXT_HTML, 134567));
    artifacts.add(makeArtifact("coll1", "auid1", "www.url2.example.com", 2,
	MediaType.TEXT_HTML, 123456));
    artifacts.add(makeArtifact("coll1", "auid1", "www.url2.example.com", 1,
	MediaType.TEXT_HTML, 112345));
    artifacts.add(makeArtifact("coll1", "auid1", "www.url1.example.com", 4,
	MediaType.TEXT_HTML, 45678));
    artifacts.add(makeArtifact("coll1", "auid1", "www.url1.example.com", 3,
	MediaType.TEXT_HTML, 34567));
    artifacts.add(makeArtifact("coll1", "auid1", "www.url1.example.com", 2,
	MediaType.TEXT_HTML, 23456));
    artifacts.add(makeArtifact("coll1", "auid1", "www.url1.example.com", 1,
	MediaType.TEXT_HTML, 12345));
    artifacts.add(makeArtifact("coll2", "auid1", "www.url4.example.com", 1,
	MediaType.TEXT_HTML, 312345));
    artifacts.add(makeArtifact("coll2", "auid1", "www.url3.example.com", 1,
	MediaType.TEXT_HTML, 212345));
    artifacts.add(makeArtifact("coll2", "auid1", "www.url2.example.com", 1,
	MediaType.TEXT_HTML, 112345));
    artifacts.add(makeArtifact("coll2", "auid1", "www.url1.example.com", 1,
	MediaType.TEXT_HTML, 12345));

    // Get exact CDX records for www.url1.example.com in the first collection.
    String collId = "coll1";
    String url = "www.url1.example.com";
    CdxRecords records = new CdxRecords();
    new CdxApiServiceImpl(null).getCdxRecords(collId, url, repository, false,
	null, null, null, records);

    // Validate count.
    assertEquals(4, records.getCdxRecordCount());

    // Validate each result.
    long previousTimestamp = -1;
    long previousCollectiondate = -1;

    // Loop through all the results.
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      // Result.
      CdxRecord cdxRecord = records.getCdxRecords().get(i);

      // Expected matching artifact.
      Artifact artifact = artifacts.get(8 - i);

      // Validate collection.
      assertEquals(collId, artifact.getCollection());

      // Validate version.
      assertEquals(4 - i, artifact.getVersion().intValue());

      // Validate URL.
      assertEquals(url, artifact.getUri());
      assertEquals(url, cdxRecord.getUrl());

      // Validate archive name.
      assertEquals(ServiceImplUtil.getArtifactArchiveName(collId,
	  artifact.getId()), cdxRecord.getArchiveName());

      // Validate timestamp sorting in ascending order.
      assertTrue(previousCollectiondate < artifact.getCollectionDate());
      assertTrue(previousTimestamp < cdxRecord.getTimestamp());
      previousCollectiondate = artifact.getCollectionDate();
      previousTimestamp = cdxRecord.getTimestamp();
    }

    // Get exact CDX records for www.url2.example.com in the first collection.
    collId = "coll1";
    url = "www.url2.example.com";
    records = new CdxRecords();
    new CdxApiServiceImpl(null).getCdxRecords(collId, url, repository, false,
	null, null, null, records);

    // Validate count.
    assertEquals(3, records.getCdxRecordCount());

    // Validate each result.
    previousTimestamp = -1;
    previousCollectiondate = -1;

    // Loop through all the results.
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      // Result.
      CdxRecord cdxRecord = records.getCdxRecords().get(i);

      // Expected matching artifact.
      Artifact artifact = artifacts.get(4 - i);

      // Validate collection.
      assertEquals(collId, artifact.getCollection());

      // Validate version.
      assertEquals(3 - i, artifact.getVersion().intValue());

      // Validate URL.
      assertEquals(url, artifact.getUri());
      assertEquals(url, cdxRecord.getUrl());

      // Validate archive name.
      assertEquals(ServiceImplUtil.getArtifactArchiveName(collId,
	  artifact.getId()), cdxRecord.getArchiveName());

      // Validate timestamp sorting in ascending order.
      assertTrue(previousCollectiondate < artifact.getCollectionDate());
      assertTrue(previousTimestamp < cdxRecord.getTimestamp());
      previousCollectiondate = artifact.getCollectionDate();
      previousTimestamp = cdxRecord.getTimestamp();
    }

    // Get exact CDX records for www.url3.example.com in the first collection.
    collId = "coll1";
    url = "www.url3.example.com";
    records = new CdxRecords();
    new CdxApiServiceImpl(null).getCdxRecords(collId, url, repository, false,
	null, null, null, records);

    // Validate count.
    assertEquals(1, records.getCdxRecordCount());

    // Result.
    CdxRecord cdxRecord = records.getCdxRecords().get(0);

    // Expected matching artifact.
    Artifact artifact = artifacts.get(1);

    // Validate collection.
    assertEquals(collId, artifact.getCollection());

    // Validate URL.
    assertEquals(url, artifact.getUri());
    assertEquals(url, cdxRecord.getUrl());

    // Validate archive name.
    assertEquals(ServiceImplUtil.getArtifactArchiveName(collId,
	artifact.getId()), cdxRecord.getArchiveName());

    // Get prefix CDX records for www. in the first collection.
    collId = "coll1";
    url = "www.";
    records = new CdxRecords();
    new CdxApiServiceImpl(null).getCdxRecords(collId, url, repository, true,
	null, null, null, records);

    // Validate count.
    assertEquals(9, records.getCdxRecordCount());

    // Loop through all the results.
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      // Result.
      cdxRecord = records.getCdxRecords().get(i);

      // Expected matching artifact.
      artifact = artifacts.get(8 - i);

      // Validate collection.
      assertEquals(collId, artifact.getCollection());

      // Validate URL.
      assertEquals(artifact.getUri(), cdxRecord.getUrl());

      // Validate timestamp.
      assertEquals(CdxRecord.computeNumericTimestamp(
	  artifact.getCollectionDate()), cdxRecord.getTimestamp());
    }

    // Get "closest" CDX records for www.url2.example.com in the first
    // collection from the epoch.
    // Chronological order is 19700101000152, 19700101000203, 19700101000214.
    collId = "coll1";
    url = "www.url2.example.com";
    records = new CdxRecords();
    new CdxApiServiceImpl(null).getCdxRecords(collId, url, repository, false,
	null, null, "19700101000000", records);

    // Validate count.
    assertEquals(3, records.getCdxRecordCount());

    // Loop through all the results.
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      // Result.
      cdxRecord = records.getCdxRecords().get(i);

      // Expected matching artifact.
      artifact = artifacts.get(4 - i);

      // Validate collection.
      assertEquals(collId, artifact.getCollection());

      // Validate URL.
      assertEquals(artifact.getUri(), cdxRecord.getUrl());

      // Validate timestamp.
      assertEquals(CdxRecord.computeNumericTimestamp(
	  artifact.getCollectionDate()), cdxRecord.getTimestamp());
    }

    // This the chronological order.
    List<CdxRecord> cdxRecordsByTimestamp = records.getCdxRecords();

    // Get "closest" CDX records for www.url2.example.com in the first
    // collection from right before the first chronological record.
    records = new CdxRecords();
    new CdxApiServiceImpl(null).getCdxRecords(collId, url, repository, false,
	null, null, "19700101000150", records);

    // Validate count.
    assertEquals(3, records.getCdxRecordCount());

    // Verify that the records are in the chronological order.
    assertIterableEquals(cdxRecordsByTimestamp, records.getCdxRecords());

    // Get "closest" CDX records for www.url2.example.com in the first
    // collection from right after the first chronological record.
    records = new CdxRecords();
    new CdxApiServiceImpl(null).getCdxRecords(collId, url, repository, false,
	null, null, "19700101000154", records);

    // Validate count.
    assertEquals(3, records.getCdxRecordCount());

    // Verify that the records are in the chronological order.
    assertIterableEquals(cdxRecordsByTimestamp, records.getCdxRecords());

    // Get "closest" CDX records for www.url2.example.com in the first
    // collection from right before the second chronological record.
    records = new CdxRecords();
    new CdxApiServiceImpl(null).getCdxRecords(collId, url, repository, false,
	null, null, "19700101000201", records);

    // Validate count.
    assertEquals(3, records.getCdxRecordCount());

    // Verify the order of the records: 2, 1, 3.
    assertEquals(cdxRecordsByTimestamp.get(1), records.getCdxRecords().get(0));
    assertEquals(cdxRecordsByTimestamp.get(0), records.getCdxRecords().get(1));
    assertEquals(cdxRecordsByTimestamp.get(2), records.getCdxRecords().get(2));

    // Get "closest" CDX records for www.url2.example.com in the first
    // collection from right after the second chronological record.
    records = new CdxRecords();
    new CdxApiServiceImpl(null).getCdxRecords(collId, url, repository, false,
	null, null, "19700101000205", records);

    // Validate count.
    assertEquals(3, records.getCdxRecordCount());

    // Verify the order of the records: 2, 3, 1.
    assertEquals(cdxRecordsByTimestamp.get(1), records.getCdxRecords().get(0));
    assertEquals(cdxRecordsByTimestamp.get(2), records.getCdxRecords().get(1));
    assertEquals(cdxRecordsByTimestamp.get(0), records.getCdxRecords().get(2));

    // Get "closest" CDX records for www.url2.example.com in the first
    // collection from right before the third chronological record.
    records = new CdxRecords();
    new CdxApiServiceImpl(null).getCdxRecords(collId, url, repository, false,
	null, null, "19700101000212", records);

    // Validate count.
    assertEquals(3, records.getCdxRecordCount());

    // Verify the order of the records: 3, 2, 1.
    assertEquals(cdxRecordsByTimestamp.get(2), records.getCdxRecords().get(0));
    assertEquals(cdxRecordsByTimestamp.get(1), records.getCdxRecords().get(1));
    assertEquals(cdxRecordsByTimestamp.get(0), records.getCdxRecords().get(2));

    // Get "closest" CDX records for www.url2.example.com in the first
    // collection from right after the third chronological record.
    records = new CdxRecords();
    new CdxApiServiceImpl(null).getCdxRecords(collId, url, repository, false,
	null, null, "19700101000216", records);

    // Validate count.
    assertEquals(3, records.getCdxRecordCount());

    // Verify the order of the records: 3, 2, 1.
    assertEquals(cdxRecordsByTimestamp.get(2), records.getCdxRecords().get(0));
    assertEquals(cdxRecordsByTimestamp.get(1), records.getCdxRecords().get(1));
    assertEquals(cdxRecordsByTimestamp.get(0), records.getCdxRecords().get(2));
  }

  /**
   * Creates an ArtifactData object.
   * 
   * @param id
   *          A String with the artifact identifier.
   * @param collection
   *          A String with the artifact collection identifier.
   * @param auid
   *          A String with the artifact AUID.
   * @param url
   *          A String with the artifact URL.
   * @param version
   *          An Integer with the artifact version.
   * @param contentType
   *          A MediaType with the artifact content type.
   * @param collectionDate
   *          A long with the artifact collection date.
   * @return an ArtifactData with the newly created object.
   */
  private ArtifactData makeArtifactData(String id, String collection,
      String auid, String url, Integer version, MediaType contentType,
      long collectionDate) {
    ArtifactIdentifier ai =
	new ArtifactIdentifier(id, collection, auid, url, version);

    HttpHeaders am = new HttpHeaders();
    am.setContentType(contentType);
    am.setDate(collectionDate);

    InputStream is = new ByteArrayInputStream(
	("WARC/1.0\r\n" + 
	"\r\n" + 
	"HTTP/1.1 200 OK\r\n" + 
	"\r\n" + 
	"<!doctype html>\r\n" + 
	"\r\n" + 
	"\r\n"
	).getBytes());

    StatusLine sl =
	new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, null);

    ArtifactData ad = new ArtifactData(ai, am, is, sl);
    ad.setCollectionDate(collectionDate);

    return ad;
  }

  /**
   * Creates an artifact and adds it to the repository.
   * 
   * @param collection
   *          A String with the artifact collection identifier.
   * @param auid
   *          A String with the artifact AUID.
   * @param url
   *          A String with the artifact URL.
   * @param version
   *          An Integer with the artifact version.
   * @param contentType
   *          A MediaType with the artifact content type.
   * @param collectionDate
   *          A long with the artifact collection date.
   * @return an Artifact with the newly created artifact.
   * @exception IOException
   *              if there are problems creating the artifact.
   */
  private Artifact makeArtifact(String collection, String auid, String url,
      Integer version, MediaType contentType, long collectionDate)
	  throws IOException {
    Artifact art = repository.addArtifact(makeArtifactData(null, collection,
	auid, url, version, contentType, collectionDate));

    return repository.commitArtifact("coll1", art.getId());
  }
}
