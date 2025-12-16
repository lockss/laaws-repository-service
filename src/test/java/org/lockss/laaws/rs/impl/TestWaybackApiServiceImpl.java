/*

Copyright (c) 2000-2025, Board of Trustees of Leland Stanford Jr. University

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

package org.lockss.laaws.rs.impl;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.HttpHost;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.lockss.app.LockssDaemon;
import org.lockss.laaws.rs.impl.WaybackApiServiceImpl.ClosestArtifact;
import org.lockss.laaws.rs.model.CdxRecord;
import org.lockss.laaws.rs.model.CdxRecords;
import org.lockss.log.L4JLogger;
import org.lockss.rs.VolatileLockssRepository;
import org.lockss.spring.test.SpringLockssTestCase4;
import org.lockss.util.rest.repo.LockssRepository;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactData;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.time.TimeBase;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.client.response.MockRestResponseCreators;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;

/**
 * Test class for org.lockss.laaws.rs.impl.WaybackApiServiceImpl.
 */
public class TestWaybackApiServiceImpl extends SpringLockssTestCase4 {
  private static L4JLogger log = L4JLogger.getLogger();

  private LockssRepository repository;
  private static final String MOCK_REST_CFGSVC = "localhost:8080";

  /**
   * Sets up a test repository.
   * 
   * @throws Exception
   *           if there are problems creating the repository.
   */
  @Before
  public void setUpArtifactDataStore() throws Exception {
    getMockLockssDaemon().setServiceBindings("cfg=" + MOCK_REST_CFGSVC);
    getMockLockssDaemon().setAppRunning(true);
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

  protected boolean wantTempTmpDir() {
    return true;
  }

  @Test
  @Ignore
  public void testRemoteRepository() throws Exception {
    long startms = TimeBase.nowMs();

    String tmpl = "http://dev2.lockss.org:24610/wayback/cdx/owb/{namespace}";

    UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(tmpl);
    Map<String, Object> uriParams = new HashMap<>();
    uriParams.put("namespace", "lockss");
    builder.uriVariables(uriParams);

//    builder.queryParam("q", "url:https://muse.jhu.edu/js/pre.js");
    builder.queryParam("q", "url:https://muse.jhu.edu/js/references.js");
    builder.queryParam("count", 10000);
    builder.queryParam("start_page", 1);

    HttpComponentsClientHttpRequestFactory clientHttpRequestFactory = new HttpComponentsClientHttpRequestFactory(
        HttpClientBuilder.create()
            .setProxy(new HttpHost("localhost", 3128))
            .build());

    RestTemplate restTemplate = new RestTemplate(clientHttpRequestFactory);
    URI url = builder.build().toUri();

    HttpHeaders hdrs = new HttpHeaders();
    hdrs.setBasicAuth("username", "password");

    RequestEntity<Void> request = RequestEntity.get(url)
        .headers(hdrs)
        .build();

    ResponseEntity<String> response =
        restTemplate.exchange(request, String.class);

    if (response.getStatusCode().isError()) {
      log.error("response.status = {}", response.getStatusCode());
    }

    log.info("response.body =\n{}", prettyPrintXml(response.getBody()));
    log.info("response.timeMs = {}", TimeBase.msSince(startms));
  }

  public static String prettyPrintXml(String xmlString) throws ParserConfigurationException, IOException, SAXException, TransformerException {
    // 1. Parse the XML string into a Document object using DocumentBuilder
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    Document doc = db.parse(new InputSource(new StringReader(xmlString)));

    // 2. Create a TransformerFactory and configure it for pretty printing
    TransformerFactory tf = TransformerFactory.newInstance();
    // This attribute ensures proper indentation with a specified indent-number
//    tf.setAttribute("indent-number", 2); // Set indentation to 2 spaces

    // 3. Create a Transformer and set output properties for indentation
    Transformer transformer = tf.newTransformer();
    transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
    transformer.setOutputProperty(OutputKeys.INDENT, "yes"); // Enable indentation
    transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8"); // Specify encoding
    // Optional: Omit XML declaration if not needed
    // transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

    // 4. Transform the Document to a StringWriter for output
    StringWriter writer = new StringWriter();
    transformer.transform(new DOMSource(doc), new StreamResult(writer));

    return writer.toString();
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

    CdxRecord cdxRecord = new WaybackApiServiceImpl(null).getCdxRecord(ad);

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
    List<Artifact> sortedArtifacts = new WaybackApiServiceImpl(null)
	.getArtifactsSortedByTemporalGap(artifacts.iterator(),
	    "19700101000001");

    assertEquals(1, sortedArtifacts.get(0).getVersion().intValue());
    assertEquals(2, sortedArtifacts.get(1).getVersion().intValue());
    assertEquals(3, sortedArtifacts.get(2).getVersion().intValue());

    // Test closest to date immediately after the first artifact.
    sortedArtifacts = new WaybackApiServiceImpl(null)
	.getArtifactsSortedByTemporalGap(artifacts.iterator(),
	    "19700101020000");

    assertEquals(1, sortedArtifacts.get(0).getVersion().intValue());
    assertEquals(2, sortedArtifacts.get(1).getVersion().intValue());
    assertEquals(3, sortedArtifacts.get(2).getVersion().intValue());

    // Test closest to date immediately before the second artifact.
    sortedArtifacts = new WaybackApiServiceImpl(null)
	.getArtifactsSortedByTemporalGap(artifacts.iterator(),
	    "19700101070000");

    assertEquals(2, sortedArtifacts.get(0).getVersion().intValue());
    assertEquals(1, sortedArtifacts.get(1).getVersion().intValue());
    assertEquals(3, sortedArtifacts.get(2).getVersion().intValue());

    // Test closest to date immediately after the second artifact.
    sortedArtifacts = new WaybackApiServiceImpl(null)
	.getArtifactsSortedByTemporalGap(artifacts.iterator(),
	    "19700101090000");

    assertEquals(2, sortedArtifacts.get(0).getVersion().intValue());
    assertEquals(3, sortedArtifacts.get(1).getVersion().intValue());
    assertEquals(1, sortedArtifacts.get(2).getVersion().intValue());

    // Test closest to date immediately before the third artifact.
    sortedArtifacts = new WaybackApiServiceImpl(null)
	.getArtifactsSortedByTemporalGap(artifacts.iterator(),
	    "19700101120000");

    assertEquals(3, sortedArtifacts.get(0).getVersion().intValue());
    assertEquals(2, sortedArtifacts.get(1).getVersion().intValue());
    assertEquals(1, sortedArtifacts.get(2).getVersion().intValue());

    // Test closest to date immediately after all artifacts.
    sortedArtifacts = new WaybackApiServiceImpl(null)
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
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, null, null, records);

    assertEquals(artifacts.size(), records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    List<CdxRecord> allCdxRecords = records.getCdxRecords();

    // Get sets of CDX records that include all of them.
    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, null, 1, records);
    assertIterableEquals(allCdxRecords, records.getCdxRecords());

    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, null, 2, records);
    assertIterableEquals(allCdxRecords, records.getCdxRecords());

    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 10000, 1, records);
    assertIterableEquals(allCdxRecords, records.getCdxRecords());

    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, artifacts.size(), 1, records);
    assertIterableEquals(allCdxRecords, records.getCdxRecords());

    // Get sets of CDX records with a page size of 8.
    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 8, 1, records);

    assertEquals(8, records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 8, 2, records);

    assertEquals(1, records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i + 8).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    // Get sets of CDX records with a page size of 4.
    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 4, 1, records);

    assertEquals(4, records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 4, 2, records);

    assertEquals(4, records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i + 4).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 4, 3, records);

    assertEquals(1, records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i + 8).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    // Get sets of CDX records with a page size of 3.
    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 3, 1, records);

    assertEquals(3, records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 3, 2, records);

    assertEquals(3, records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i + 3).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 3, 3, records);

    assertEquals(3, records.getCdxRecordCount());
    for (int i = 0; i < records.getCdxRecordCount(); i++) {
      assertEquals(artifacts.get(i + 6).getUri(),
	  records.getCdxRecords().get(i).getUrl());
    }

    // Get sets of CDX records with a page size of 1.
    for (int pageIdx = 0; pageIdx < 9; pageIdx++) {
      records = new CdxRecords();
      new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	  repository, 1, pageIdx + 1, records);

      assertEquals(1, records.getCdxRecordCount());
      assertEquals(artifacts.get(pageIdx).getUri(),
	  records.getCdxRecords().get(0).getUrl());
    }

    // Get empty results beyond the last CDX record.
    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 10000, 2, records);
    assertEquals(0, records.getCdxRecordCount());

    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, artifacts.size(), 2, records);
    assertEquals(0, records.getCdxRecordCount());

    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 8, 3, records);
    assertEquals(0, records.getCdxRecordCount());

    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 4, 4, records);
    assertEquals(0, records.getCdxRecordCount());

    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 3, 4, records);
    assertEquals(0, records.getCdxRecordCount());

    records = new CdxRecords();
    new WaybackApiServiceImpl(null).getArtifactsCdxRecords(artifacts.iterator(),
	repository, 1, 10, records);
    assertEquals(0, records.getCdxRecordCount());
  }

  private class MyWaybackApiServiceImpl extends WaybackApiServiceImpl {
    private String serviceUser;
    private String servicePassword;

    public MyWaybackApiServiceImpl(HttpServletRequest request) {
      super(request);
      this.setUsernameAndPassword("test.username", "test.password");
    }

    @Override
    protected HttpHeaders getAuthHeaders() {
      HttpHeaders hdrs = new HttpHeaders();
      LockssDaemon daemon = LockssDaemon.getLockssDaemon();
      daemon.getRestClientCredentials();
      hdrs.setBasicAuth(serviceUser, servicePassword);
      return hdrs;
    }

    protected WaybackApiServiceImpl setUsernameAndPassword(String username, String password) {
      serviceUser = username;
      servicePassword = password;
      return this;
    }
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

    artifacts.add(makeArtifact("coll1", "auid1", "www.url4.example.com", null, MediaType.TEXT_HTML, 312345));
    artifacts.add(makeArtifact("coll1", "auid1", "www.url3.example.com", null, MediaType.TEXT_HTML, 212345));
    artifacts.add(makeArtifact("coll1", "auid1", "www.url2.example.com", null, MediaType.TEXT_HTML, 134567));
    artifacts.add(makeArtifact("coll1", "auid1", "www.url2.example.com", null, MediaType.TEXT_HTML, 123456));
    artifacts.add(makeArtifact("coll1", "auid1", "www.url2.example.com", null, MediaType.TEXT_HTML, 112345));

    artifacts.add(makeArtifact("coll1", "auid1", "www.url1.example.com", null, MediaType.TEXT_HTML, 45678));
    artifacts.add(makeArtifact("coll1", "auid1", "www.url1.example.com", null, MediaType.TEXT_HTML, 34567));
    artifacts.add(makeArtifact("coll1", "auid1", "www.url1.example.com", null, MediaType.TEXT_HTML, 23456));
    artifacts.add(makeArtifact("coll1", "auid1", "www.url1.example.com", null, MediaType.TEXT_HTML, 12345));

    artifacts.add(makeArtifact("coll2", "auid1", "www.url4.example.com", null, MediaType.TEXT_HTML, 312345));
    artifacts.add(makeArtifact("coll2", "auid1", "www.url3.example.com", null, MediaType.TEXT_HTML, 212345));
    artifacts.add(makeArtifact("coll2", "auid1", "www.url2.example.com", null, MediaType.TEXT_HTML, 112345));
    artifacts.add(makeArtifact("coll2", "auid1", "www.url1.example.com", null, MediaType.TEXT_HTML, 12345));

    RestTemplate restTemplate = new RestTemplate();
    MockRestServiceServer mockServer = MockRestServiceServer.createServer(restTemplate);

    // Setup mock for the first four REST calls
    for (int i = 0; i < 4; i++) {
      String url = "www.url" + (i + 1) + ".example.com";
      mockServer.expect(
              requestTo("http://" + MOCK_REST_CFGSVC + "/utils/normalizeurl?url=" + url))
          .andRespond(
              MockRestResponseCreators.withSuccess(
                  "[\"" + url + "\"]",
                  MediaType.APPLICATION_JSON));
    }

    // Get exact CDX records for www.url1.example.com in the first collection.
    String collId = "coll1";
    String url = "www.url1.example.com";

    CdxRecords records = new CdxRecords();

    new MyWaybackApiServiceImpl(null)
        .getCdxRecords0(restTemplate, collId, url, repository, false, null, null, null, records);

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
      assertEquals(collId, artifact.getNamespace());

      // Validate version.
      assertEquals(4 - i, artifact.getVersion().intValue());

      // Validate URL.
      assertEquals(url, artifact.getUri());
      assertEquals(url, cdxRecord.getUrl());

      // Validate archive name.
      assertEquals(
          ServiceImplUtil.getArtifactArchiveName(collId, artifact.getUuid()),
          cdxRecord.getArchiveName()
      );

      // Validate timestamp sorting in ascending order.
      log.trace("previousCollectionDate = {}", previousCollectiondate);
      log.trace("artifact.getCollectionDate = {}", artifact.getCollectionDate());

      assertTrue(previousCollectiondate < artifact.getCollectionDate());
      assertTrue(previousTimestamp < cdxRecord.getTimestamp());
      previousCollectiondate = artifact.getCollectionDate();
      previousTimestamp = cdxRecord.getTimestamp();
    }

    // Get exact CDX records for www.url2.example.com in the first collection.
    collId = "coll1";
    url = "www.url2.example.com";
    records = new CdxRecords();
    new MyWaybackApiServiceImpl(null)
        .getCdxRecords0(restTemplate, collId, url, repository, false,
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
      assertEquals(collId, artifact.getNamespace());

      // Validate version.
      assertEquals(3 - i, artifact.getVersion().intValue());

      // Validate URL.
      assertEquals(url, artifact.getUri());
      assertEquals(url, cdxRecord.getUrl());

      // Validate archive name.
      assertEquals(ServiceImplUtil.getArtifactArchiveName(collId,
	  artifact.getUuid()), cdxRecord.getArchiveName());

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
    new MyWaybackApiServiceImpl(null)
        .getCdxRecords0(restTemplate, collId, url, repository, false,
	null, null, null, records);

    // Validate count.
    assertEquals(1, records.getCdxRecordCount());

    // Result.
    CdxRecord cdxRecord = records.getCdxRecords().get(0);

    // Expected matching artifact.
    Artifact artifact = artifacts.get(1);

    // Validate collection.
    assertEquals(collId, artifact.getNamespace());

    // Validate URL.
    assertEquals(url, artifact.getUri());
    assertEquals(url, cdxRecord.getUrl());

    // Validate archive name.
    assertEquals(ServiceImplUtil.getArtifactArchiveName(collId,
	artifact.getUuid()), cdxRecord.getArchiveName());

    // Setup mock for the next REST calls
    mockServer.reset();
    mockServer.expect(
            requestTo("http://" + MOCK_REST_CFGSVC + "/utils/normalizeurl?url=www."))
        .andRespond(
            MockRestResponseCreators.withSuccess(
                "[\"www.\"]",
                MediaType.APPLICATION_JSON));

    // Get prefix CDX records for www. in the first collection.
    collId = "coll1";
    url = "www.";
    records = new CdxRecords();
    new MyWaybackApiServiceImpl(null)
        .getCdxRecords0(restTemplate, collId, url, repository, true,
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
      assertEquals(collId, artifact.getNamespace());

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

    // Setup mock for the next seven REST calls to Configuration Service
    mockServer.reset();
    for (int i = 0; i < 7; i++) {
      mockServer.expect(
              requestTo("http://" + MOCK_REST_CFGSVC + "/utils/normalizeurl?url=" + url))
          .andRespond(
              MockRestResponseCreators.withSuccess(
                  "[\"" + url + "\"]",
                  MediaType.APPLICATION_JSON));
    }

    records = new CdxRecords();
    new MyWaybackApiServiceImpl(null)
        .getCdxRecords0(restTemplate, collId, url, repository, false,
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
      assertEquals(collId, artifact.getNamespace());

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
    new MyWaybackApiServiceImpl(null)
        .getCdxRecords0(restTemplate, collId, url, repository, false,
	null, null, "19700101000150", records);

    // Validate count.
    assertEquals(3, records.getCdxRecordCount());

    // Verify that the records are in the chronological order.
    assertIterableEquals(cdxRecordsByTimestamp, records.getCdxRecords());

    // Get "closest" CDX records for www.url2.example.com in the first
    // collection from right after the first chronological record.
    records = new CdxRecords();
    new MyWaybackApiServiceImpl(null)
        .getCdxRecords0(restTemplate, collId, url, repository, false,
	null, null, "19700101000154", records);

    // Validate count.
    assertEquals(3, records.getCdxRecordCount());

    // Verify that the records are in the chronological order.
    assertIterableEquals(cdxRecordsByTimestamp, records.getCdxRecords());

    // Get "closest" CDX records for www.url2.example.com in the first
    // collection from right before the second chronological record.
    records = new CdxRecords();
    new MyWaybackApiServiceImpl(null)
        .getCdxRecords0(restTemplate, collId, url, repository, false,
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
    new MyWaybackApiServiceImpl(null)
        .getCdxRecords0(restTemplate, collId, url, repository, false,
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
    new MyWaybackApiServiceImpl(null)
        .getCdxRecords0(restTemplate, collId, url, repository, false,
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
    new MyWaybackApiServiceImpl(null)
        .getCdxRecords0(restTemplate, collId, url, repository, false,
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

    HttpHeaders am = new HttpHeaders();
    am.setContentType(contentType);
    am.setDate(collectionDate);

    ArtifactSpec spec = new ArtifactSpec()
        .setArtifactUuid(id)
        .setNamespace(collection)
        .setAuid(auid)
        .setUrl(url)
        .setVersion(version)
        .setHeaders(am.toSingleValueMap())
        .setCollectionDate(collectionDate);

    spec.generateContent();

    return spec.getArtifactData();
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
   * @return an Artifact with the newly created artifact.
   * @exception IOException
   *              if there are problems creating the artifact.
   */
  private Artifact makeArtifact(String collection,
                                String auid,
                                String url,
                                Integer version,
                                MediaType contentType,
                                long collectionDate) throws IOException {

    // This is ignored by addArtifact() but created here for makeArtifactData()
//    long collectionDate = Instant.now().toEpochMilli();

    // Add artifact
    Artifact art = repository
        .addArtifact(makeArtifactData(null, collection, auid, url, version, contentType, collectionDate));

    // Commit artifact
    return repository.commitArtifact("coll1", art.getUuid());
  }
}
