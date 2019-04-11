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
import org.junit.runner.RunWith;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.core.VolatileLockssRepository;
import org.lockss.laaws.rs.impl.CdxApiServiceImpl.ClosestArtifact;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.CdxRecord;
import org.lockss.laaws.rs.model.CdxRecords;
import org.lockss.util.test.LockssTestCase5;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Test class for org.lockss.laaws.rs.impl.CdxApiServiceImpl.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class TestCdxApiServiceImpl extends LockssTestCase5 {
  @Autowired
  ApplicationContext appCtx;

  @TestConfiguration
  static class TestLockssRepositoryConfig {
    @Bean
    public LockssRepository createInitializedRepository() throws IOException {
      LockssRepository repository = new VolatileLockssRepository();
      repository.initRepository();
      return repository;
    }
  }

  LockssRepository repository;

  /**
   * Sets up a test repository.
   * 
   * @throws Exception
   *           if there are problems creating the repository.
   */
  @Before
  public void setUpArtifactDataStore() throws Exception {
    this.repository = makeLockssRepository();
  }

  /**
   * Tears down the repository after each test.
   */
  @After
  public void tearDownArtifactDataStore() {
      this.repository = null;
  }

  /**
   * Creates a test repository.
   * 
   * @return a LockssRepository with the test repository newly created.
   * @throws Exception
   *           if there are problems creating the repository.
   */
  public LockssRepository makeLockssRepository() throws Exception {
    LockssRepository repository = new VolatileLockssRepository();
    repository.initRepository();
    return repository;
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
    Artifact art1 = makeArtifact("coll1", "auid1", "url1", 1,
	MediaType.TEXT_HTML, 0);

    ClosestArtifact ca = new ClosestArtifact(art1, 12345);
    assertEquals(art1, ca.getArtifact());
    assertEquals(12345, ca.getGap());

    art1.setCollectionDate(12345);

    assertEquals(0, new ClosestArtifact(art1, 12345).getGap());
    assertEquals(1000, new ClosestArtifact(art1, 11345).getGap());
    assertEquals(1000, new ClosestArtifact(art1, 13345).getGap());
  }

  /**
   * Tests the creation of a CDX record.
   *
   * @exception Exception
   *              if there are problems creating the CDX record.
   */
  @Test
  public void testGetCdxRecord() throws Exception {
    ArtifactIdentifier ai1 =
	new ArtifactIdentifier("id1", "coll1", "auid1", "url1.example.com", 1);

    HttpHeaders am1 = new HttpHeaders();
    am1.setContentType(MediaType.TEXT_HTML);

    InputStream is1 = new ByteArrayInputStream("test".getBytes());

    StatusLine sl1 =
	new BasicStatusLine(new ProtocolVersion("http", 1, 1), 200, null);

    ArtifactData ad1 = new ArtifactData(ai1, am1, is1, sl1);
    ad1.setCollectionDate(24 * 60 * 60 * 1000);
    ad1.setContentDigest("cd1");
    ad1.setContentLength(9876);

    CdxRecord cdxRecord = new CdxApiServiceImpl(null).getCdxRecord(ad1);

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
    List<Artifact> artifacts = new ArrayList<>();
    artifacts.add(makeArtifact("coll1", "auid1", "url1.example.com", 1,
	MediaType.TEXT_HTML, 12345));
    artifacts.add(makeArtifact("coll1", "auid1", "url1.example.com", 2,
	MediaType.TEXT_HTML, 23456));
    artifacts.add(makeArtifact("coll1", "auid1", "url1.example.com", 3,
	MediaType.TEXT_HTML, 34567));
    artifacts.add(makeArtifact("coll1", "auid1", "url2.example.com", 1,
	MediaType.TEXT_HTML, 112345));
    artifacts.add(makeArtifact("coll1", "auid1", "url2.example.com", 2,
	MediaType.TEXT_HTML, 123456));
    artifacts.add(makeArtifact("coll1", "auid1", "url2.example.com", 3,
	MediaType.TEXT_HTML, 134567));
    artifacts.add(makeArtifact("coll1", "auid1", "url3.example.com", 1,
	MediaType.TEXT_HTML, 212345));
    artifacts.add(makeArtifact("coll1", "auid1", "url3.example.com", 2,
	MediaType.TEXT_HTML, 223456));
    artifacts.add(makeArtifact("coll1", "auid1", "url3.example.com", 3,
	MediaType.TEXT_HTML, 234567));

    CdxRecords records = new CdxRecords();

    new CdxApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, null, null, records);

    assertEquals(9, records.getCdxRecordCount());
  }

  /**
   * Creates an artifact.
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
   * @param originDate
   *          A long with the artifact origin date.
   * @return an Artifact with the newly created artifact.
   * @exception IOException
   *              if there are problems creating the artifact.
   */
  private Artifact makeArtifact(String collection, String auid, String url,
      Integer version, MediaType contentType, long originDate)
	  throws IOException {
    ArtifactIdentifier ai =
	new ArtifactIdentifier(collection, auid, url, version);

    HttpHeaders am = new HttpHeaders();
    am.setContentType(contentType);

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
    ad.setCollectionDate(originDate);

    Artifact art = repository.addArtifact(ad);

    return repository.commitArtifact("coll1", art.getId());
  }
}
