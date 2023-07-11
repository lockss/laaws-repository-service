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

package org.lockss.laaws.rs.controller;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicStatusLine;
import org.archive.format.warc.WARCConstants;
import org.junit.*;
import org.junit.runner.RunWith;
import org.lockss.laaws.rs.api.ArchivesApi;
import org.lockss.laaws.rs.impl.ArtifactsApiServiceImpl;
import org.lockss.log.L4JLogger;
import org.lockss.rs.LocalLockssRepository;
import org.lockss.rs.io.index.ArtifactIndex;
import org.lockss.rs.io.storage.ArtifactDataStore;
import org.lockss.spring.test.SpringLockssTestCase4;
import org.lockss.test.ConfigurationUtil;
import org.lockss.test.LockssTestCase4;
import org.lockss.test.RandomInputStream;
import org.lockss.test.ZeroInputStream;
import org.lockss.util.ListUtil;
import org.lockss.util.PreOrderComparator;
import org.lockss.util.StringUtil;
import org.lockss.util.io.DeferredTempFileOutputStream;
import org.lockss.util.io.FileUtil;
import org.lockss.util.rest.exception.LockssRestHttpException;
import org.lockss.util.rest.repo.LockssNoSuchArtifactIdException;
import org.lockss.util.rest.repo.LockssRepository;
import org.lockss.util.rest.repo.RestLockssRepository;
import org.lockss.util.rest.repo.model.*;
import org.lockss.util.rest.repo.util.ArtifactConstants;
import org.lockss.util.rest.repo.util.ArtifactSpec;
import org.lockss.util.time.TimeBase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpHeaders;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPOutputStream;

import static java.nio.file.StandardOpenOption.APPEND;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/**
 * Tests an embedded LOCKSS Repository Service instance configured with an internal {@link LocalLockssRepository}.
 * Client requests to the embedded server are made through the {@link RestLockssRepository} client interface.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@ActiveProfiles("test")
public class TestRestLockssRepository extends SpringLockssTestCase4 {
  private final static L4JLogger log = L4JLogger.getLogger();

  protected static int MAX_RANDOM_FILE = 50000;
  protected static int MAX_INCR_FILE = 20000;
//   protected static int MAX_RANDOM_FILE = 4000;
//   protected static int MAX_INCR_FILE = 4000;

  public static AuSize AUSIZE_ZERO = new AuSize();

  static {
    AUSIZE_ZERO.setTotalLatestVersions(0L);
    AUSIZE_ZERO.setTotalAllVersions(0L);
    AUSIZE_ZERO.setTotalWarcSize(0L);
  }


  static boolean WRONG = false;

  // TEST DATA

  // Commonly used artifact identifiers and contents
  protected static String NS1 = "ns1";
  protected static String NS2 = "ns2";
  protected static String AUID1 = "auid1";
  protected static String AUID2 = "auid2";
  protected static String ARTID1 = "art_id_1";

  protected static String URL1 = "http://host1.com/path";
  protected static String URL2 = "http://host2.com/fil,1";
  protected static String URL3 = "http://host2.com/fil/2";
  protected static String LONG_URL_4000 = "http://host2.com" +
      StringUtils.repeat("/123456789", 400) + "/foo.txt";
  protected static String LONG_URL_40000 = "http://host2.com" +
      StringUtils.repeat("/123456789", 4000) + "/foo.txt";
  protected static String PREFIX1 = "http://host2.com/";

  protected static String URL_PREFIX = "http://host2.com/";

  protected static String CONTENT1 = "content string 1";

  protected static HttpHeaders HEADERS1 = new HttpHeaders();

  static {
    HEADERS1.set("key1", "val1");
    HEADERS1.set("key2", "val2");
  }

  protected static StatusLine STATUS_LINE_OK =
      new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK");
  protected static StatusLine STATUS_LINE_MOVED =
      new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 301, "Moved");

  // Identifiers expected not to exist in the repository
  protected static String NO_NAMESPACE = "no_ns";
  protected static String NO_AUID = "no_auid";
  protected static String NO_URL = "no_url";
  protected static String NO_ARTID = "not an artifact ID";

  // Sets of ns, au, url for combinatoric tests.  Last one in each
  // differs only in case from previous, to check case-sensitivity
  protected static String[] NAMESPACES = {NS1, NS2, "Coll2"};
  protected static String[] AUIDS = {AUID1, AUID2, "Auid2"};
  protected static String[] URLS = {URL1, URL2, URL2.toUpperCase(), URL3};

  @LocalServerPort
  private int port;

  static List<File> tmpDirs = new ArrayList<>();

  @Autowired
  LockssRepository internalRepo;

  @TestConfiguration
  @Profile("test")
  static class TestLockssRepositoryConfig {
    /**
     * Initializes the internal {@link LockssRepository} instance used by the
     * embedded LOCKSS Repository Service.
     */
    @Bean
    public LockssRepository createInitializedRepository() throws IOException {
      File stateDir = LockssTestCase4.getTempDir(tmpDirs);
      File basePath = LockssTestCase4.getTempDir(tmpDirs);

      LockssRepository repository =
          new LocalLockssRepository(stateDir, basePath, NS1);

      return spy(repository);
    }

    @Bean
    public ArtifactIndex setArtifactIndex() {
      return null;
    }

    @Bean
    public ArtifactDataStore setArtifactDataStore() {
      return null;
    }
  }

  @AfterClass
  public static void deleteTempDirs() throws Exception {
    LockssTestCase4.deleteTempFiles(tmpDirs);
  }

  protected RestLockssRepository repoClient;

  // ArtifactSpec for each Artifact that has been added to the repository
  List<ArtifactSpec> addedSpecs = new ArrayList<ArtifactSpec>();

  // Maps ArtButVer to ArtifactSpec for highest version added to the repository
  Map<String, ArtifactSpec> highestVerSpec = new HashMap<String, ArtifactSpec>();
  // Maps ArtButVer to ArtifactSpec for highest version added and committed to
  // the repository
  Map<String, ArtifactSpec> highestCommittedVerSpec = new HashMap<String, ArtifactSpec>();

  /**
   * Provides a newly built LOCKSS repository implemented by a remote REST
   * Repository service.
   *
   * @return a LockssRepository with the newly built LOCKSS repository.
   * @throws Exception if there are problems.
   */
  public RestLockssRepository makeLockssRepository() throws Exception {
    log.info("port = " + port);
    return new RestLockssRepository(
        new URL(String.format("http://localhost:%d", port)), null, null);
  }

  @Before
  public void setUpArtifactDataStore() throws Exception {
    TimeBase.setSimulated();
    getMockLockssDaemon().setAppRunning(true);
    internalRepo.initRepository();
    this.repoClient = makeLockssRepository();
  }

  @After
  public void tearDownArtifactDataStore() throws Exception {
    this.repoClient = null;
  }

  @Test
  public void testArtifactSizes() throws IOException {
    for (int size = 0; size < MAX_INCR_FILE; size += 100) {
      testArtifactSize(size);
    }
  }

  public void testArtifactSize(int size) throws IOException {
    ArtifactSpec spec = ArtifactSpec.forNsAuUrl(NS1, AUID1, URL1 + size)
        .toCommit(true).setContentLength(size);
    Artifact newArt = addUncommitted(spec);
    Artifact commArt = commit(spec, newArt);
    spec.assertArtifact(repoClient, commArt);
  }

  /**
   * Tests "Add Artifact" operation with an artifact larger than the default DFOS threshold.
   */
  @Test
  public void testAddArtifactDFOS() throws Exception {
    ArtifactSpec spec = new ArtifactSpec()
        .setUrl("https://example.lockss.org/foo")
        .setContentLength(FileUtils.ONE_MB * 10)
        .setCollectionDate(1234);

    Artifact artifact = addUncommitted(spec);
    Artifact committed = commit(spec, artifact);

    spec.assertArtifact(repoClient, committed);
  }

  /**
   * Tests adding and fetching artifact with an illegal mime type in
   * the Content-Type header
   */
  @Test
  public void testAddArtifactMalformedContentType() throws Exception {
    HttpHeaders headers = new HttpHeaders();
    long clen = FileUtils.ONE_KB;

    headers.add("Date", "Fri, 29 Jul 2022 21:08:40 GMT");
    headers.add("Accept-Ranges", "bytes");
    headers.add("Content-Length", String.valueOf(clen));
    headers.add("Content-Type", "x-ms-wmv");

    // Test with an HTTP response artifact
    {
      ArtifactSpec spec = new ArtifactSpec()
          .setUrl("https://example.lockss.org/foo1")
          .setContentLength(clen)
          .setCollectionDate(1234)
          .setHeaders(headers.toSingleValueMap());

      Artifact artifact = addUncommitted(spec);
      Artifact committed = commit(spec, artifact);

      spec.assertArtifact(repoClient, committed);
    }

    // Test with a resource artifact
    {
      ArtifactSpec spec = new ArtifactSpec()
          .setUrl("https://example.lockss.org/foo2")
          .setContentLength(clen)
          .setCollectionDate(1234)
          .setHeaders(headers.toSingleValueMap())
          .setIsHttpResponse(false);

      Artifact artifact = addUncommitted(spec);
      Artifact committed = commit(spec, artifact);

      spec.assertArtifact(repoClient, committed);
    }
  }

  /**
   * Tests resource Artifact, ensure it creates the correct WARC record type.
   */
  @Test
  public void testResourceArtifact() throws Exception {
    HttpHeaders headers = new HttpHeaders();

    headers.add("Content-Type", "x-ms-wmv");
    ArtifactSpec spec = new ArtifactSpec()
      .setUrl("https://example.lockss.org/foo2")
      .setContentLength(100)
      .setCollectionDate(1234)
      .setHeaders(headers.toSingleValueMap())
      .setIsHttpResponse(false);

    Artifact artifact = addUncommitted(spec);
    Artifact committed = commit(spec, artifact);
    spec.assertArtifact(repoClient, committed);
    ArtifactData ad = repoClient.getArtifactData(committed);
    assertEquals(headers.getFirst("Content-Type"),
        ad.getHttpHeaders().getFirst("Content-Type"));
    assertFalse(ad.isHttpResponse());
    Artifact copied = waitCopied(spec);
    String path = new URL(copied.getStorageUrl()).getPath();
    String warcstr = StringUtil.fromFile(path);
    assertMatchesRE("WARC-Type: resource", warcstr);
  }

  /**
   * Test for {@link ArchivesApi#addArtifacts(String, MultipartFile, String)}.
   */
  @Test
  public void testAddArtifacts() throws Exception {
    String namespace = "namespace";
    String auId = "auId";

    //// Test adding artifacts from an uncompressed WARC archive with success
    {
      int numArtifacts = 10;
      boolean isCompressed = false;
      List<ArtifactSpec> specs = new ArrayList<>(numArtifacts);

      // Write test WARC
      log.debug("Writing test WARC [numArtifacts = {}]", numArtifacts);

      DeferredTempFileOutputStream dfos =
          new DeferredTempFileOutputStream((int) FileUtils.ONE_MB, "test-warc");

      String url = "http://www.lockss.org/example1.txt";

      for (int i = 0; i < numArtifacts; i++) {
        ArtifactSpec spec = createArtifactSpec(namespace, auId, url);
        spec.setCommitted(true);
        specs.add(spec);
        writeWarcRecord(spec, dfos);
      }

      dfos.close();

      // Call addArtifacts REST endpoint
      log.debug("Calling REST addArtifacts");

      Iterable<ImportStatus> iter = repoClient.addArtifacts(
          namespace, auId, dfos.getDeleteOnCloseInputStream(), LockssRepository.ArchiveType.WARC, isCompressed);

      List<ImportStatus> result = ListUtil.fromIterable(iter);
      ImportStatus last = result.get(result.size() - 1);

      // Wait for committed artifacts to be written to permanent storage
      log.debug("Waiting for copies to finish");

      waitCopied(namespace, auId, last.getUrl());

      // Assert artifacts from the archive were added successfully
      log.debug("Asserting artifacts added from archive");

      assertEquals(specs.size(), result.size());

      for (int i = 0; i < result.size(); i++) {
        ImportStatus status = result.get(i);
        ArtifactSpec spec = specs.get(i);

        assertEquals(formatWarcRecordId(spec.getArtifactUuid()), status.getWarcId());
        assertEquals(spec.getUrl(), status.getUrl());
        assertEquals(spec.getContentDigest(), status.getDigest());
        assertEquals(ImportStatus.StatusEnum.OK, status.getStatus());
        assertNull(status.getStatusMessage());

        Artifact artifact =
            repoClient.getArtifactVersion(namespace, auId, spec.getUrl(), i+1, true);

        assertEquals(artifact.getUuid(), status.getArtifactUuid());
        assertEquals(artifact.getVersion(), status.getVersion());
        assertEquals(spec.getContentDigest(), artifact.getContentDigest());

        spec.assertArtifact(repoClient, artifact);
      }
    }

    //// Test adding artifacts from a GZIP compressed WARC archive with success
    {
      int numArtifacts = 10;
      boolean isCompressed = true;
      List<ArtifactSpec> specs = new ArrayList<>(numArtifacts);

      // Write test WARC
      log.debug("Writing test WARC [numArtifacts = {}]", numArtifacts);

      File tmpFile = FileUtil.createTempFile("test-warc", null);
      tmpFile.deleteOnExit();

      String url = "http://www.lockss.org/example2.txt";

      for (int i = 0; i < numArtifacts; i++) {
        ArtifactSpec spec = createArtifactSpec(namespace, auId, url);
        spec.setCommitted(true);
        specs.add(spec);

        // Append individual GZIP members to the file
        try (OutputStream fileOut = Files.newOutputStream(tmpFile.toPath(), APPEND)) {
          try (GZIPOutputStream gzipOutput = new GZIPOutputStream(fileOut)) {
            writeWarcRecord(spec, gzipOutput);
          }
        }
      }

      // Call addArtifacts REST endpoint
      log.debug("Calling REST addArtifacts");

      try (InputStream fileInput = Files.newInputStream(tmpFile.toPath())) {
        Iterable<ImportStatus> iter = repoClient.addArtifacts(
            namespace, auId, fileInput, LockssRepository.ArchiveType.WARC, isCompressed);

        List<ImportStatus> result = ListUtil.fromIterable(iter);

        // Wait for committed artifacts to be written to permanent storage
        log.debug("Waiting for copies to finish");

        waitCopied(namespace, auId, url);

        // Assert artifacts from the archive were added successfully
        log.debug("Asserting artifacts added from archive");

        assertEquals(specs.size(), result.size());

        for (int i = 0; i < result.size(); i++) {
          ImportStatus status = result.get(i);
          ArtifactSpec spec = specs.get(i);

          assertEquals(formatWarcRecordId(spec.getArtifactUuid()), status.getWarcId());
          assertEquals(spec.getUrl(), status.getUrl());
          assertEquals(spec.getContentDigest(), status.getDigest());
          assertEquals(ImportStatus.StatusEnum.OK, status.getStatus());
          assertNull(status.getStatusMessage());

          Artifact artifact =
              repoClient.getArtifactVersion(namespace, auId, spec.getUrl(), i + 1, true);

          assertEquals(artifact.getUuid(), status.getArtifactUuid());
          assertEquals(artifact.getVersion(), status.getVersion());
          assertEquals(spec.getContentDigest(), artifact.getContentDigest());

          spec.assertArtifact(repoClient, artifact);
        }
      }
    }

    //// Test adding artifacts from a WARC archive with only partial success
    {
      int numArtifacts = 10;
      boolean isCompressed = false;
      List<ArtifactSpec> specs = new ArrayList<>(numArtifacts);

      // Write test WARC
      log.debug("Writing test WARC [numArtifacts = {}]", numArtifacts);

      DeferredTempFileOutputStream dfos =
          new DeferredTempFileOutputStream((int) FileUtils.ONE_MB, "test-warc");

      String url = "http://www.lockss.org/example3.txt";

      for (int i = 0; i < numArtifacts; i++) {
        String artifactUrl = (i % 2 == 0) ? "error" : url;
        ArtifactSpec spec = createArtifactSpec(namespace, auId, artifactUrl);
        spec.setCommitted(true);
        specs.add(spec);
        writeWarcRecord(spec, dfos);
      }

      dfos.close();

      // Setup mock to throw IOException if URL contains "error"
      doThrow(new IOException("This is a mocked IOException; okay to ignore"))
          .when(internalRepo)
          .addArtifact(argThat(
              artifactData -> artifactData.getIdentifier().getUri().contains("error")));

      // Call addArtifacts REST endpoint
      Iterable<ImportStatus> iter = repoClient.addArtifacts(
          namespace, auId, dfos.getDeleteOnCloseInputStream(), LockssRepository.ArchiveType.WARC, isCompressed);

      List<ImportStatus> result = ListUtil.fromIterable(iter);

      log.debug("result = {}", result);

      // Wait for committed artifacts to be written to permanent storage
      log.debug("Waiting for copies to finish");

      waitCopied(namespace, auId, url);

      // Assert artifacts from the archive were added successfully
      log.debug("Asserting artifacts added from archive");

      assertEquals(specs.size(), result.size());

      for (int i = 0; i < result.size(); i++) {
        ImportStatus status = result.get(i);
        ArtifactSpec spec = specs.get(i);

        assertEquals(formatWarcRecordId(spec.getArtifactUuid()), status.getWarcId());
        assertEquals(spec.getUrl(), status.getUrl());

        if (i % 2 == 0) {
          assertEquals("i="+i, ImportStatus.StatusEnum.ERROR, status.getStatus());
        } else {
          assertEquals(spec.getContentDigest(), status.getDigest());
          assertEquals(ImportStatus.StatusEnum.OK, status.getStatus());
          assertNull(status.getStatusMessage());
        }
      }

      // Assert successfully added artifacts
      for (int version = 1; version <= 5; version++) {
        ImportStatus status = result.get(version*2-1);
        ArtifactSpec spec = specs.get(version*2-1);

        Artifact artifact =
            repoClient.getArtifactVersion(namespace, auId, spec.getUrl(), version, true);

        assertEquals(artifact.getUuid(), status.getArtifactUuid());
        assertEquals(artifact.getVersion(), status.getVersion());
        assertEquals(spec.getContentDigest(), artifact.getContentDigest());

        spec.assertArtifact(repoClient, artifact);
      }
    }
  }

  /**
   * Test for {@link ArchivesApi#addArtifacts(String, MultipartFile, String)}.
   */
  @Test
  public void testAddArtifactsDupCheck() throws Exception {
    String namespace = "namespace1";
    String auId = "auId1";

    // Make two WARCs, with one URL unquely in each and two in common,
    // one with different content, one with same

    String url = "http://www.lockss.org/example.";
    String url1 = url + "1";
    String url2 = url + "2";
    String url3 = url + "3";
    String url4 = url + "4";

    ArtifactSpec asu1c1 = createArtifactSpec(namespace, auId, url1);
    // Need two version of content for url2
    ArtifactSpec asu2c1 = createArtifactSpec(namespace, auId, url2);
    ArtifactSpec asu2c2 = createArtifactSpec(namespace, auId, url2);
    ArtifactSpec asu3c1 = createArtifactSpec(namespace, auId, url3);
    ArtifactSpec asu4c1 = createArtifactSpec(namespace, auId, url4);

    DeferredTempFileOutputStream dfos1 =
      new DeferredTempFileOutputStream((int)FileUtils.ONE_MB, "test-warc1");
    writeWarcRecord(asu1c1, dfos1);
    writeWarcRecord(asu2c1, dfos1);
    writeWarcRecord(asu3c1, dfos1);
    dfos1.close();

    DeferredTempFileOutputStream dfos2 =
      new DeferredTempFileOutputStream((int)FileUtils.ONE_MB, "test-warc2");
    writeWarcRecord(asu2c2, dfos2);     // different content
    writeWarcRecord(asu3c1, dfos2);     // same conteng
    writeWarcRecord(asu4c1, dfos2);
    dfos2.close();

    // import the two warcs
    log.debug("Calling REST addArtifacts");
    Iterable<ImportStatus> iter1 =
      repoClient.addArtifacts(namespace, auId,
                              dfos1.getDeleteOnCloseInputStream(),
                              LockssRepository.ArchiveType.WARC,
                              false);
    List<ImportStatus> results1 = ListUtil.fromIterable(iter1);
    assertEquals(3, results1.size());
    assertEquals(ImportStatus.StatusEnum.OK, results1.get(0).getStatus());
    assertEquals(ImportStatus.StatusEnum.OK, results1.get(1).getStatus());
    assertEquals(ImportStatus.StatusEnum.OK, results1.get(2).getStatus());

    Iterable<ImportStatus> iter2 =
      repoClient.addArtifacts(namespace, auId,
                              dfos2.getDeleteOnCloseInputStream(),
                              LockssRepository.ArchiveType.WARC,
                              false);

    List<ImportStatus> results2 = ListUtil.fromIterable(iter2);
    assertEquals(3, results2.size());
    assertEquals(ImportStatus.StatusEnum.OK, results2.get(0).getStatus());
    assertEquals(ImportStatus.StatusEnum.DUPLICATE, results2.get(1).getStatus());
    assertEquals(ImportStatus.StatusEnum.OK, results2.get(2).getStatus());

    assertEquals(1, artver(repoClient.getArtifact(namespace, auId, url1)));
    assertEquals(2, artver(repoClient.getArtifact(namespace, auId, url2)));
    assertEquals(1, artver(repoClient.getArtifact(namespace, auId, url3)));
    assertEquals(1, artver(repoClient.getArtifact(namespace, auId, url4)));

    // Now import the 2nd WARC again with storeDuplicate true, ensure
    // get new version of dup artifact
    Iterable<ImportStatus> iter3 =
      repoClient.addArtifacts(namespace, auId,
                              dfos2.getDeleteOnCloseInputStream(),
                              LockssRepository.ArchiveType.WARC,
                              false, true);

    List<ImportStatus> results3 = ListUtil.fromIterable(iter3);
    assertEquals(3, results3.size());
    assertEquals(ImportStatus.StatusEnum.OK, results3.get(0).getStatus());
    assertEquals(ImportStatus.StatusEnum.OK, results3.get(1).getStatus());
    assertEquals(ImportStatus.StatusEnum.OK, results3.get(2).getStatus());

    assertEquals(1, artver(repoClient.getArtifact(namespace, auId, url1)));
    assertEquals(3, artver(repoClient.getArtifact(namespace, auId, url2)));
    assertEquals(2, artver(repoClient.getArtifact(namespace, auId, url3)));
    assertEquals(2, artver(repoClient.getArtifact(namespace, auId, url4)));
  }

  private int artver(Artifact art) {
    return art.getVersion();
  }

  private String formatWarcRecordId(String uuid) {
    return "<urn:uuid:" + uuid + ">";
  }

  private String formatWarcDate(long warcDate) {
    return DateTimeFormatter.ISO_INSTANT
        .format(Instant.ofEpochMilli(warcDate).atZone(ZoneOffset.UTC));
  }

  private ArtifactSpec createArtifactSpec(String namespace, String auId, String url) {
    long contentLength = 128 * FileUtils.ONE_KB;
    HttpHeaders headers = new HttpHeaders();

    headers.add("Date", "Fri, 29 Jul 2022 21:08:40 GMT");
    headers.add("Accept-Ranges", "bytes");
    headers.add("Content-Length", String.valueOf(contentLength));
    headers.add("Content-Type", "text/plain");
    headers.add("key1", "val1");
    headers.add("key2", "val2");

    int seed = new Random().nextInt();

    return new ArtifactSpec()
        .setArtifactUuid(UUID.randomUUID().toString())
        .setNamespace(namespace)
        .setAuid(auId)
        .setUrl(url)
        .setHeaders(headers.toSingleValueMap())
        .setCollectionDate(TimeBase.nowMs())
        .setContentLength(contentLength)
        .setContentGenerator(() ->
            new BoundedInputStream(new RandomInputStream(seed), contentLength));
  }

  private void writeWarcRecord(ArtifactSpec spec, OutputStream out) throws IOException {
    String WARC_BLOCK =
        "HTTP/1.1 200 OK\n" +
            spec.getHeadersAsText() +
            "\n";

    long WARC_BLOCK_LENGTH = WARC_BLOCK.length() + spec.getContentLength();

    String WARC_HEADER = "WARC/1.0" + WARCConstants.CRLF +
        "WARC-Record-ID: " + formatWarcRecordId(spec.getArtifactUuid()) + WARCConstants.CRLF +
        "WARC-Target-URI: " + spec.getUrl() + WARCConstants.CRLF +
        "Content-Length: " + WARC_BLOCK_LENGTH + WARCConstants.CRLF +
        "WARC-Date: " + formatWarcDate(spec.getCollectionDate()) + WARCConstants.CRLF +
        "WARC-Type: response" + WARCConstants.CRLF +
//          "WARC-Block-Digest: sha1:UZY6ND6CCHXETFVJD2MSS7ZENMWF7KQ2\n" +
//          "WARC-Payload-Digest: sha1:CCHXETFVJD2MUZY6ND6SS7ZENMWF7KQ2\n" +
        "Content-Type: application/http;msgtype=response" + WARCConstants.CRLF +
        // FIXME: Content-Type is hard coded
        "WARC-Identified-Payload-Type: text/plain" + WARCConstants.CRLF +
        // These two fields avoid WarcArtifactDataStore#writeArtifactData() from exhausting the InputStream
        // to determine the value of these properties
        ArtifactConstants.ARTIFACT_LENGTH_KEY + ": " + spec.getContentLength() + WARCConstants.CRLF +
        ArtifactConstants.ARTIFACT_DIGEST_KEY + ": " + spec.getContentDigest() + WARCConstants.CRLF +
        WARCConstants.CRLF;

    out.write(WARC_HEADER.getBytes(StandardCharsets.UTF_8));

    out.write(WARC_BLOCK.getBytes(StandardCharsets.UTF_8));

//    IOUtils.copyLarge(spec.getInputStream(), out, 0, spec.getContentLength());
    IOUtils.copyLarge(spec.getInputStream(), out);

    out.write(WARCConstants.CRLF.getBytes());
    out.write(WARCConstants.CRLF.getBytes());

    out.flush();
  }

  @Test
  public void testAddArtifactResource() throws Exception {
    ArtifactSpec spec = new ArtifactSpec()
        .setNamespace("namespace")
        .setAuid("auid")
        .setUrl("url")
        .setStatusLine(null)
        .generateContent();

    Artifact artifact = repoClient.addArtifact(spec.getArtifactData());

    spec.assertArtifact(repoClient, artifact);
  }

  @Test
  public void testAddArtifact() throws IOException {
    // Illegal arguments
    assertThrowsMatch(IllegalArgumentException.class,
        "ArtifactData",
        () -> repoClient.addArtifact(null));

    // Illegal ArtifactData (at least one null field)
    for (ArtifactData illAd : nullPointerArtData) {
      assertThrows(NullPointerException.class,
          () -> repoClient.addArtifact(illAd));
    }

    // legal use of addArtifact is tested in the normal course of setting
    // up variants, and by testArtifactSizes(), but for the sake of
    // completeness ...

    ArtifactSpec spec = new ArtifactSpec().setUrl("https://mr/ed/").setContent(CONTENT1).setCollectionDate(0);
    Artifact newArt = addUncommitted(spec);
    Artifact commArt = commit(spec, newArt);
    spec.assertArtifact(repoClient, commArt);
  }

  @Test
  public void testAddArtifact_badRequest() throws Exception {
    // Try adding an artifact with no URL.
    assertThrowsMatch(LockssRestHttpException.class,
        "400 Bad Request: addArtifact",
        () -> {
          addUncommitted(new ArtifactSpec().setUrl(null));
        });
  }

  private static final long LARGE_ARTIFACT_SIZE = (2L * FileUtils.ONE_GB) + (1L * FileUtils.ONE_MB); // 2049 MiB

  /**
   * Exercises storing and retrieving a large artifact to then from a remote Repository Service via a
   * {@link RestLockssRepository} client.
   *
   * @throws Exception
   */
  @Ignore
  @Test
  public void testLargeArtifactStorageAndRetrieval() throws Exception {
    // Large artifact spec
    ArtifactSpec spec =
        new ArtifactSpec()
            .setContentGenerator(() -> new ZeroInputStream((byte) 46, LARGE_ARTIFACT_SIZE))
            .setContentLength(LARGE_ARTIFACT_SIZE)
            // SHA-256 hash of LARGE_ARTIFACT_SIZE * decimal 46 precalculated to speed-up test:
            .setContentDigest("SHA-256:3b3b50f71c9cc1647819e090594cf191977413f79a8bfa0b473f468cf74dcb3e")
            // FIXME: Provide defaults for these in ArtifactSpec:
            .setAuid("auid")
            .setUrl("dots")
            .setCollectionDate(0);

    // Add large artifact to remote Repository service
    Artifact artifact = repoClient.addArtifact(spec.getArtifactData());

    // Retrieve the large artifact from the remote Repository service
    ArtifactData ad = repoClient.getArtifactData(artifact);

    // Assert artifact data against spec
    spec.assertArtifactData(ad);
  }

  @Test
  public void testAddLargeArtifact() throws IOException {
    long len = 100 * 1024 * 1024;
    ArtifactSpec spec =
        new ArtifactSpec()
            .setUrl("https://mr/ed/")
            .setContentGenerator(() -> new ZeroInputStream((byte) 27, len))
            .setCollectionDate(0);

    Artifact newArt = addUncommitted(spec);
    String storeUrl = newArt.getStorageUrl();

    log.info("uncommArt.getStorageUrl(): " + storeUrl);

    Artifact commArt = repoClient.commitArtifact(spec.getNamespace(), newArt.getUuid());
    spec.setCommitted(true);

    log.info("commArt.getStorageUrl(): " + commArt.getStorageUrl());

    if (!commArt.getStorageUrl().equals(storeUrl)) {
      // The storage URL should not change until the background copy has
      // completely, which should take significant time.  If it has changed
      // already that might be an indication that the copy happened
      // synchronously
      log.warn("Storage URL of huge Artifact changed immediately after commit: "
          + commArt.getStorageUrl());
    }

    // Ensure that the artifact eventually moves from temp to perm WARC and
    // that it's still correct after that happens.

    // Might not see change in storageUrl due to Artifact cache, so disable it
    repoClient.enableArtifactCache(false, null);

    while (commArt.getStorageUrl().equals(storeUrl)) {
      commArt = repoClient.getArtifact(spec.getNamespace(), spec.getAuid(), spec.getUrl());
      log.info("commArt.getStorageUrl(): " + commArt.getStorageUrl());
    }

    spec.assertArtifact(repoClient, commArt);
  }

  @Test
  public void testAddLongUrl() throws IOException {
    ArtifactSpec spec1 =
        new ArtifactSpec()
            .setUrl(LONG_URL_4000)
            .setContent(CONTENT1)
            .setCollectionDate(0);
    Artifact newArt1 = addUncommitted(spec1);
    Artifact commArt1 = commit(spec1, newArt1);
    spec1.assertArtifact(repoClient, commArt1);

    ArtifactSpec spec2 =
        new ArtifactSpec()
            .setUrl(LONG_URL_40000)
            .setContent(StringUtils.repeat(CONTENT1, 1000))
            .setCollectionDate(0);
    Artifact newArt2 = addUncommitted(spec2);
    Artifact commArt2 = commit(spec2, newArt2);
    spec2.assertArtifact(repoClient, commArt2);
  }

  @Test
  public void emptyRepo() throws IOException {
    checkEmptyAu(NS1, AUID1);
  }

  void checkEmptyAu(String ns, String auid) throws IOException {
    assertEmpty(repoClient.getAuIds(ns));
    assertEmpty(repoClient.getNamespaces());
    assertEmpty(repoClient.getArtifacts(ns, AUID1));

    assertNull(repoClient.getArtifact(ns, AUID1, URL1));

    assertEquals(AUSIZE_ZERO, repoClient.auSize(ns, AUID1));

    // FIXME: This is ugly - need any Artifact to trigger the mock error response
    ArtifactSpec missingSpec = new ArtifactSpec()
        .setNamespace("ns1")
        .setArtifactUuid("artifactid1")
        .setUrl("test")
        .setContentLength(1)
        .setContentDigest("test")
        .setCollectionDate(0);

    assertThrows(LockssNoSuchArtifactIdException.class,
        () -> repoClient.getArtifactData(missingSpec.getArtifact()));

    assertEmpty(repoClient.getArtifactsAllVersions(ns, AUID1, URL1));
  }

  @Test
  public void testNoSideEffect() throws Exception {
    for (StdVariants var : StdVariants.values()) {
      instantiateScanario(var.toString());
      testAllNoSideEffect();
    }
  }

  public void testFromAllAusMethods() throws IOException {
    testGetArtifactsWithUrlFromAllAus();
    testGetArtifactsWithUrlPrefixFromAllAus();
  }

  public void testAllNoSideEffect() throws Exception {
    testGetArtifact();
    testGetArtifactData();
    testGetArtifactVersion();
    testAuSize();
    testGetAllArtifacts();
    testGetAllArtifactsWithPrefix();
    testGetAllArtifactsAllVersions();
    testGetAllArtifactsWithPrefixAllVersions();
    testGetArtifactAllVersions();
    testGetArtifactsWithUrlFromAllAus();
    testGetArtifactsWithUrlPrefixFromAllAus();
    testGetAuIds();
    testGetNamespaces();
//    testIsArtifactCommitted();
  }

  @Test
  public void testModifications() throws Exception {
    testCommitArtifact();
    testDeleteArtifact();
    instantiateScanario(getVariantSpecs("commit1"));
    instantiateScanario(getVariantSpecs("uncommit1"));
    instantiateScanario(getVariantSpecs("disjoint"));
    testCommitArtifact();
    testDeleteArtifact();
    testAllNoSideEffect();
    testDeleteAllArtifacts();
  }

  /** Test for {@link RestLockssRepository#getArtifactData(Artifact, LockssRepository.IncludeContent)}. */
  @Test
  public void testConditionalContent() throws IOException {
    runTestConditionalContent(false);
    runTestConditionalContent(true);
  }

  public void runTestConditionalContent(boolean useMultipartEndpoint) throws IOException {
    repoClient.setUseMultipartEndpoint(useMultipartEndpoint);

    // Set the threshold to the default threshold
    ConfigurationUtil.addFromArgs(ArtifactsApiServiceImpl.PARAM_SMALL_CONTENT_THRESHOLD,
        String.valueOf(ArtifactsApiServiceImpl.DEFAULT_SMALL_CONTENT_THRESHOLD));

    String url_small = "https://art/small/";
    String url_large = "https://art/large/";
    String url_larger = "https://art/larger/";
    long def_small = ArtifactsApiServiceImpl.DEFAULT_SMALL_CONTENT_THRESHOLD;
    long len_small = def_small / 2;
    long len_large = def_small * 5;
    long len_larger = def_small * 10;
    ArtifactSpec spec_small =
        new ArtifactSpec()
            .setUrl(url_small)
            .setContentGenerator(() -> new ZeroInputStream((byte) 27, len_small))
            .setCollectionDate(0);
    ArtifactSpec spec_large =
        new ArtifactSpec()
            .setUrl(url_large)
            .setContentGenerator(() -> new ZeroInputStream((byte) 27, len_large))
            .setCollectionDate(0);
    ArtifactSpec spec_larger =
        new ArtifactSpec()
            .setUrl(url_larger)
            .setContentGenerator(() -> new ZeroInputStream((byte) 27, len_larger))
            .setCollectionDate(0);
    Artifact art_small = addUncommitted(spec_small);
    Artifact art_large = addUncommitted(spec_large);
    Artifact art_larger = addUncommitted(spec_larger);
    Artifact art_small_c =
        repoClient.commitArtifact(spec_small.getNamespace(), art_small.getUuid());
    Artifact art_large_c =
        repoClient.commitArtifact(spec_large.getNamespace(), art_large.getUuid());
    Artifact art_larger_c =
        repoClient.commitArtifact(spec_larger.getNamespace(), art_larger.getUuid());
    spec_small.setCommitted(true);
    spec_large.setCommitted(true);
    spec_larger.setCommitted(true);

    assertReceivesNoContent(art_small_c, LockssRepository.IncludeContent.NEVER);
    assertReceivesContent(art_small_c, LockssRepository.IncludeContent.IF_SMALL);
    assertReceivesContent(art_small_c, LockssRepository.IncludeContent.ALWAYS);

    assertReceivesNoContent(art_large_c, LockssRepository.IncludeContent.NEVER);
    assertReceivesNoContent(art_large_c, LockssRepository.IncludeContent.IF_SMALL);
    assertReceivesContent(art_large_c, LockssRepository.IncludeContent.ALWAYS);

    assertReceivesNoContent(art_larger_c, LockssRepository.IncludeContent.NEVER);
    assertReceivesNoContent(art_larger_c, LockssRepository.IncludeContent.IF_SMALL);
    assertReceivesContent(art_larger_c, LockssRepository.IncludeContent.ALWAYS);

    // Set the threshold to something larger
    ConfigurationUtil.addFromArgs(ArtifactsApiServiceImpl.PARAM_SMALL_CONTENT_THRESHOLD,
        "" + (len_large + len_larger) / 2);

    assertReceivesNoContent(art_small_c, LockssRepository.IncludeContent.NEVER);
    assertReceivesContent(art_small_c, LockssRepository.IncludeContent.IF_SMALL);
    assertReceivesContent(art_small_c, LockssRepository.IncludeContent.ALWAYS);

    assertReceivesNoContent(art_large_c, LockssRepository.IncludeContent.NEVER);
    assertReceivesContent(art_large_c, LockssRepository.IncludeContent.IF_SMALL);
    assertReceivesContent(art_large_c, LockssRepository.IncludeContent.ALWAYS);

    assertReceivesNoContent(art_larger_c, LockssRepository.IncludeContent.NEVER);
    assertReceivesNoContent(art_larger_c, LockssRepository.IncludeContent.IF_SMALL);
    assertReceivesContent(art_larger_c, LockssRepository.IncludeContent.ALWAYS);
  }

  // Ensure artifact names can be arbitrary strings (not nec. URL).
  @Test
  public void testNonUrlName() throws IOException {
    // Pairs of (name, content).
    Pair[] nameContPairs =
      { Pair.of("bilbo.zip", "lots of round things"),
        Pair.of("who uses names like this?", "windows users"),
        Pair.of("  ", "might as well be pathological"),
        Pair.of("   ", "might as well be even more pathological")};
    List<String> names = new ArrayList<>();
    // Create and check Artifact for each pair
    for (Pair<String,String> pair : nameContPairs) {
      names.add(pair.getLeft());
      ArtifactSpec spec = new ArtifactSpec()
        .setUrl(pair.getLeft())
        .setContent(pair.getRight())
        .setCollectionDate(0);
      Artifact newArt = addUncommitted(spec);
      Artifact commArt = commit(spec, newArt);
      spec.assertArtifact(repoClient, commArt);
      spec.assertArtifact(repoClient, getArtifact(repoClient, spec, false));
    }
    // Enumerate the Artifacts, check that names are as expected
    Collections.sort(names);
    assertIterableEquals(names,
                         StreamSupport.stream(repoClient.getArtifacts(NS1, AUID1).spliterator(), false)
                         .map(x -> x.getUri())
                         .collect(Collectors.toList()));
  }

  /**
   * Assert that the repo supplies content with the ArtifactData
   */
  void assertReceivesContent(Artifact art, LockssRepository.IncludeContent ic)
      throws IOException {
    ArtifactData ad = repoClient.getArtifactData(art, ic);
    assertTrue(ad.hasContentInputStream());
  }

  /**
   * Assert that the repo does not supply content with the ArtifactData
   */
  void assertReceivesNoContent(Artifact art, LockssRepository.IncludeContent ic)
      throws IOException {
    ArtifactData ad = repoClient.getArtifactData(art, ic);
    assertFalse(ad.hasContentInputStream());
  }

  public void testGetArtifact() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
        "Null AUID",
        () -> repoClient.getArtifact(NS1, null, URL1));
    assertThrowsMatch(IllegalArgumentException.class,
        "Null AUID or URL",
        () -> repoClient.getArtifact(NS1, AUID1, null));

    // Artifact not found
    for (ArtifactSpec spec : notFoundArtifactSpecs()) {
      log.info("s.b. notfound: " + spec);
      assertNull("Null or non-existent name shouldn't be found: " + spec,
          getArtifact(repoClient, spec, false));
    }

    // Ensure that a no-version retrieval gets the expected highest version
    for (ArtifactSpec highSpec : highestCommittedVerSpec.values()) {
      log.info("highSpec: " + highSpec);
      highSpec.assertArtifact(repoClient, repoClient.getArtifact(
          highSpec.getNamespace(),
          highSpec.getAuid(),
          highSpec.getUrl()));
    }

  }

  /** Test for {@link RestLockssRepository#getArtifactData(Artifact)}. */
  public void testGetArtifactData() throws Exception {
    runTestGetArtifactData(false);
    runTestGetArtifactData(true);
  }

  private void runTestGetArtifactData(boolean useMultipartEndpoint) throws Exception {
    repoClient.setUseMultipartEndpoint(useMultipartEndpoint);

    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
        "Null artifact",
        () -> repoClient.getArtifactData(null));

    // FIXME: This is ugly - need any Artifact to trigger the mock error response
    ArtifactSpec missingSpec = new ArtifactSpec()
        .setNamespace("ns1")
        .setArtifactUuid("artifactid1")
        .setUrl("test")
        .setContentLength(1)
        .setContentDigest("test")
        .setCollectionDate(0);

    // Artifact ID not found
    assertThrowsMatch(LockssNoSuchArtifactIdException.class,
        "Artifact not found",
        () -> repoClient.getArtifactData(missingSpec.getArtifact()));

    ArtifactSpec cspec = anyCommittedSpec();
    if (cspec != null) {
      ArtifactData ad = repoClient.getArtifactData(cspec.getArtifact());
      cspec.assertArtifactData(ad);
    }

    ArtifactSpec uspec = anyUncommittedSpec();
    if (uspec != null) {
      ArtifactData ad = repoClient.getArtifactData(uspec.getArtifact());
      uspec.assertArtifactData(ad);
    }
  }

  public void testGetArtifactVersion() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
        "Null AUID",
        () -> repoClient.getArtifactVersion(null, null, null, null, false));
    assertThrowsMatch(IllegalArgumentException.class,
        "Null AUID",
        () -> repoClient.getArtifactVersion(null, null, null, null, true));
    assertThrowsMatch(IllegalArgumentException.class,
        "Null AUID",
        () -> repoClient.getArtifactVersion(NS1, null, URL1, 1, false));
    assertThrowsMatch(IllegalArgumentException.class,
        "Null AUID",
        () -> repoClient.getArtifactVersion(NS1, null, URL1, 1, true));
    assertThrowsMatch(IllegalArgumentException.class,
        "URL",
        () -> repoClient.getArtifactVersion(NS1, AUID1, null, 1, false));
    assertThrowsMatch(IllegalArgumentException.class,
        "URL",
        () -> repoClient.getArtifactVersion(NS1, AUID1, null, 1, true));
    assertThrowsMatch(IllegalArgumentException.class,
        "version",
        () -> repoClient.getArtifactVersion(NS1, AUID1, URL1, null, false));
    assertThrowsMatch(IllegalArgumentException.class,
        "version",
        () -> repoClient.getArtifactVersion(NS1, AUID1, URL1, null, true));
    // XXXAPI illegal version numbers
//     assertThrowsMatch(IllegalArgumentException.class,
// 		      "version",
// 		      () -> {repository.getArtifactVersion(NS1, AUID1, URL1, -1);});
//     assertThrowsMatch(IllegalArgumentException.class,
// 		      "version",
// 		      () -> {repository.getArtifactVersion(NS1, AUID1, URL1, 0);});

    // Artifact not found

    // notFoundArtifactSpecs() includes some that would be found with a
    // different version so can't use that here.

    for (ArtifactSpec spec : neverFoundArtifactSpecs) {
      log.info("s.b. notfound: " + spec);
      assertNull("Null or non-existent name shouldn't be found: " + spec,
          getArtifactVersion(repoClient, spec, 1, false));
      assertNull("Null or non-existent name shouldn't be found: " + spec,
          getArtifactVersion(repoClient, spec, 1, true));
      assertNull("Null or non-existent name shouldn't be found: " + spec,
          getArtifactVersion(repoClient, spec, 2, false));
      assertNull("Null or non-existent name shouldn't be found: " + spec,
          getArtifactVersion(repoClient, spec, 2, true));
    }

    // Get all added artifacts, check correctness
    for (ArtifactSpec spec : addedSpecs) {
      spec.assertArtifact(repoClient, getArtifact(repoClient, spec, true));

      if (spec.isCommitted()) {
        log.info("s.b. data: " + spec);
        spec.assertArtifact(repoClient, getArtifact(repoClient, spec, false));
      } else {
        log.info("s.b. uncommitted: " + spec);
        assertNull("Uncommitted shouldn't be found: " + spec,
            getArtifact(repoClient, spec, false));
      }
      // XXXAPI illegal version numbers
      assertNull(getArtifactVersion(repoClient, spec, 0, false));
      assertNull(getArtifactVersion(repoClient, spec, 0, true));
      assertNull(getArtifactVersion(repoClient, spec, -1, false));
      assertNull(getArtifactVersion(repoClient, spec, -1, true));
    }

    // Ensure that a non-existent version isn't found
    for (ArtifactSpec highSpec : highestVerSpec.values()) {
      log.info("highSpec: " + highSpec);
      assertNull(repoClient.getArtifactVersion(highSpec.getNamespace(),
          highSpec.getAuid(),
          highSpec.getUrl(),
          highSpec.getVersion() + 1,
          false));
      assertNull(repoClient.getArtifactVersion(highSpec.getNamespace(),
          highSpec.getAuid(),
          highSpec.getUrl(),
          highSpec.getVersion() + 1,
          true));
    }
  }

  public void testAuSize() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
        "Null AUID",
        () -> repoClient.auSize(null, null));
    assertThrowsMatch(IllegalArgumentException.class,
        "Null AUID",
        () -> repoClient.auSize(NS1, null));

    // non-existent AU
    assertEquals(AUSIZE_ZERO, repoClient.auSize(NO_NAMESPACE, NO_AUID));

    // Calculate the expected size of each AU in each namespace, compare with auSize()
    for (String ns : addedNamespaces()) {
      for (String auid : addedAuids()) {
        long expTotalAllVersions = committedSpecStream()
            .filter(spec -> spec.getAuid().equals(auid))
            .filter(spec -> spec.getNamespace().equals(ns))
            .mapToLong(ArtifactSpec::getContentLength)
            .sum();

        long expTotalLatestVersions = highestCommittedVerSpec.values().stream()
            .filter(spec -> spec.getAuid().equals(auid))
            .filter(spec -> spec.getNamespace().equals(ns))
            .mapToLong(ArtifactSpec::getContentLength)
            .sum();

        long expTotalWarcSize = repoClient.auSize(ns, auid).getTotalWarcSize();

        AuSize auSize = repoClient.auSize(ns, auid);

        assertEquals((long) expTotalAllVersions, (long) auSize.getTotalAllVersions());
        assertEquals((long) expTotalLatestVersions, (long) auSize.getTotalLatestVersions());

        // FIXME: We don't actually remove anything from disk yet so we expect the size to be the same
        assertEquals(expTotalWarcSize, (long) auSize.getTotalWarcSize());
      }
    }
  }

  public void testCommitArtifact() throws IOException {
    // Illegal args
    assertThrows(IllegalArgumentException.class,
                 () -> repoClient.commitArtifact(null, null));
    assertThrows(IllegalArgumentException.class,
                 () -> repoClient.commitArtifact(NS1, null));

    // Artifact not found in default namespace
    assertThrows(LockssNoSuchArtifactIdException.class,
                 () -> repoClient.commitArtifact(null, ARTID1));

    // Commit already committed artifact
    ArtifactSpec commSpec = anyCommittedSpec();

    if (commSpec != null) {
      // Get the existing artifact
      Artifact commArt = getArtifact(repoClient, commSpec, false);
      assertTrue(commSpec.isCommitted());
      assertTrue(commArt.isCommitted());
      // XXXAPI should this throw?
//       assertThrows(NullPointerException.class,
// 		   () -> {repository.commitArtifact(commSpec.getNamespace(),
// 						    commSpec.getArtifactId());});
      Artifact dupArt = repoClient.commitArtifact(commSpec.getNamespace(),
          commSpec.getArtifactUuid());
      assertEquals(commArt, dupArt);

      commSpec.assertArtifact(repoClient, dupArt);

      // Get the same artifact when uncommitted may be included.
      commArt = getArtifact(repoClient, commSpec, true);
      assertEquals(commArt, dupArt);

      assertThrowsMatch(LockssNoSuchArtifactIdException.class,
          "non-existent artifact",
          () -> repoClient.commitArtifact(commSpec.getNamespace(),
              "NOTANARTID"));
    }
  }

  public void testDeleteArtifact() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
        "Null artifact UUID",
        () -> repoClient.deleteArtifact(null, null));
    assertThrowsMatch(IllegalArgumentException.class,
        "artifact",
        () -> repoClient.deleteArtifact(NS1, null));

    // Artifact not found in default namespace
    assertThrowsMatch(LockssNoSuchArtifactIdException.class,
        "Could not remove artifact id: " + NO_ARTID,
        () -> repoClient.deleteArtifact(null, NO_ARTID));

    // Delete non-existent artifact
    // XXXAPI
    assertThrowsMatch(LockssNoSuchArtifactIdException.class,
        "Could not remove artifact id: " + NO_ARTID,
        () -> repoClient.deleteArtifact(NO_NAMESPACE, NO_ARTID));

    {
      // Delete a committed artifact that isn't the highest version. it
      // should disappear but size shouldn't change
      ArtifactSpec spec = committedSpecStream()
          .filter(s -> s != highestCommittedVerSpec.get(s.artButVerKey()))
          .findAny().orElse(null);
      if (spec != null) {
        AuSize auSize1 = repoClient.auSize(spec.getNamespace(), spec.getAuid());

        assertNotNull(repoClient.getArtifactData(spec.getArtifact()));
        assertNotNull(getArtifact(repoClient, spec, false));
        assertNotNull(getArtifact(repoClient, spec, true));
        log.info("Deleting not highest: " + spec);
        repoClient.deleteArtifact(spec.getNamespace(), spec.getArtifactUuid());

        assertThrows(LockssNoSuchArtifactIdException.class,
            () -> repoClient.getArtifactData(spec.getArtifact()));

        assertNull(getArtifact(repoClient, spec, false));
        assertNull(getArtifact(repoClient, spec, true));
        delFromAll(spec);

        AuSize auSize2 = repoClient.auSize(spec.getNamespace(), spec.getAuid());

        // Assert totalLatestVersions remains the same but totalAllVersions is different
        assertEquals("Latest versions size changed after deleting non-highest version",
            auSize1.getTotalLatestVersions(), auSize2.getTotalLatestVersions());
        assertNotEquals("All versions size did NOT change after deleting non-highest version",
            auSize1.getTotalAllVersions(), auSize2.getTotalAllVersions());

        assertEquals(auSize1.getTotalWarcSize(), auSize2.getTotalWarcSize());
      }
    }
    {
      // Delete a highest-version committed artifact, it should disappear and
      // size should change
      ArtifactSpec spec = highestCommittedVerSpec.values().stream()
          .findAny().orElse(null);
      if (spec != null) {
        AuSize auSize1 = repoClient.auSize(spec.getNamespace(), spec.getAuid());

        long artsize = spec.getContentLength();
        assertNotNull(repoClient.getArtifactData(spec.getArtifact()));
        assertNotNull(getArtifact(repoClient, spec, false));
        assertNotNull(getArtifact(repoClient, spec, true));
        log.info("Deleting highest: " + spec);
        repoClient.deleteArtifact(spec.getNamespace(), spec.getArtifactUuid());

        assertThrows(LockssNoSuchArtifactIdException.class,
            () -> repoClient.getArtifactData(spec.getArtifact()));

        assertNull(getArtifact(repoClient, spec, false));
        assertNull(getArtifact(repoClient, spec, true));
        delFromAll(spec);

        long expectedTotalAllVersions = auSize1.getTotalAllVersions() - artsize;
        long expectedTotalLatestVersions = auSize1.getTotalLatestVersions() - artsize;
        long expectedTotalWarcSize = repoClient.auSize(spec.getNamespace(), spec.getAuid()).getTotalWarcSize();

        ArtifactSpec newHigh = highestCommittedVerSpec.get(spec.artButVerKey());
        if (newHigh != null) {
          expectedTotalLatestVersions += newHigh.getContentLength();
        }

        AuSize auSize2 = repoClient.auSize(spec.getNamespace(), spec.getAuid());

        assertEquals("AU all artifact versions size wrong after deleting highest version",
            (long) expectedTotalAllVersions, (long) auSize2.getTotalAllVersions());
        assertEquals("AU latest artifact versions size wrong after deleting highest version",
            (long) expectedTotalLatestVersions, (long) auSize2.getTotalLatestVersions());

        // FIXME: We don't actually remove anything from disk yet so we expect the size to be the same
        assertEquals("AU WARC size total wrong after deleting highest version",
            expectedTotalWarcSize, (long) auSize2.getTotalWarcSize());
      }
    }
    // Delete an uncommitted artifact, it should disappear and size should
    // not change
    {
      ArtifactSpec uspec = anyUncommittedSpec();
      if (uspec != null) {
        AuSize auSize1 = repoClient.auSize(uspec.getNamespace(), uspec.getAuid());

        assertNotNull(repoClient.getArtifactData(uspec.getArtifact()));
        assertNull(getArtifact(repoClient, uspec, false));
        assertNotNull(getArtifact(repoClient, uspec, true));
        log.info("Deleting uncommitted: " + uspec);
        repoClient.deleteArtifact(uspec.getNamespace(), uspec.getArtifactUuid());

        assertThrows(LockssNoSuchArtifactIdException.class,
            () -> repoClient.getArtifactData(uspec.getArtifact()));

        assertNull(getArtifact(repoClient, uspec, false));
        assertNull(getArtifact(repoClient, uspec, true));
        delFromAll(uspec);

        AuSize auSize2 = repoClient.auSize(uspec.getNamespace(), uspec.getAuid());

        assertEquals("AU size changed after deleting uncommitted", auSize1, auSize2);
      }
    }
  }

  public void testDeleteAllArtifacts() throws IOException {
    // TK Delete committed & uncommitted arts & check results each time
    // delete twice
    // check getAuIds() & getNamespaces() as they run out
    Iterator<String> nsIter = repoClient.getNamespaces().iterator();

    // Loop through all the artifacts.
    Iterator<ArtifactSpec> iter =
        (Iterator<ArtifactSpec>) addedSpecStream().iterator();

    while (iter.hasNext()) {
      // Get the next artifact.
      ArtifactSpec spec = iter.next();
      String ns = spec.getNamespace();
      String id = spec.getArtifactUuid();
      assertNotNull(repoClient.getArtifactData(spec.getArtifact()));
      // Delete the artifact.
      repoClient.deleteArtifact(ns, id);

      assertThrows(LockssNoSuchArtifactIdException.class,
          () -> repoClient.getArtifactData(spec.getArtifact()));

      // Delete it again.
      try {
        repoClient.deleteArtifact(ns, id);
        fail("Should have thrown LockssNoSuchArtifactIdException");
      } catch (LockssNoSuchArtifactIdException iae) {
      }

      assertThrows(LockssNoSuchArtifactIdException.class,
          () -> repoClient.getArtifactData(spec.getArtifact()));
    }

    // There are no namespaces now.
    assertFalse(repoClient.getNamespaces().iterator().hasNext());

    // There are no AUIds in any of the previously existing namespaces.
    while (nsIter.hasNext()) {
      assertFalse(repoClient.getAuIds(nsIter.next()).iterator().hasNext());
    }
  }

  public void testGetAllArtifacts() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
        "Null AUID",
        () -> repoClient.getArtifacts(null, null));
    assertThrowsMatch(IllegalArgumentException.class,
        "Null AUID",
        () -> repoClient.getArtifacts(NS1, null));

    // Non-existent namespace & auid
    assertEmpty(repoClient.getArtifacts(NO_NAMESPACE, NO_AUID));

    String anyColl = null;
    String anyAuid = null;

    // Compare with all URLs in each AU
    for (String ns : addedNamespaces()) {
      anyColl = ns;
      for (String auid : addedAuids()) {
        anyAuid = auid;
        ArtifactSpec.assertArtList(repoClient, (orderedAllAu(ns, auid)
                .filter(distinctByKey(ArtifactSpec::artButVerKey))),
            repoClient.getArtifacts(ns, auid));
      }
    }

    // Combination of ns and au id that both exist, but have no artifacts
    // in common
    Pair<String, String> nsAu = nsAuMismatch();
    if (nsAu != null) {
      assertEmpty(repoClient.getArtifacts(nsAu.getLeft(),
          nsAu.getRight()));
    }
    // non-existent ns, au
    if (anyColl != null && anyAuid != null) {
      assertEmpty(repoClient.getArtifacts(anyColl,
          anyAuid + "_notAuSuffix"));
      assertEmpty(repoClient.getArtifacts(anyColl + "_notCollSuffix",
          anyAuid));
    }
  }

  public void testGetAllArtifactsWithPrefix() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
        "Null AUID or URL prefix",
        () -> repoClient.getArtifactsWithPrefix(null, null, null));
    assertThrowsMatch(IllegalArgumentException.class,
        "prefix",
        () -> repoClient.getArtifactsWithPrefix(NS1, AUID1, null));
    assertThrowsMatch(IllegalArgumentException.class,
        "Null AUID",
        () -> repoClient.getArtifactsWithPrefix(NS1, null, PREFIX1));

    // Non-existent namespace & auid
    assertEmpty(repoClient.getArtifactsWithPrefix(NO_NAMESPACE, NO_AUID, PREFIX1));
    // Compare with all URLs matching prefix in each AU
    for (String ns : addedNamespaces()) {
      for (String auid : addedAuids()) {
        ArtifactSpec.assertArtList(repoClient, (orderedAllAu(ns, auid)
                .filter(spec -> spec.getUrl().startsWith(PREFIX1))
                .filter(distinctByKey(ArtifactSpec::artButVerKey))),
            repoClient.getArtifactsWithPrefix(ns, auid, PREFIX1));
        assertEmpty(repoClient.getArtifactsWithPrefix(ns, auid,
            PREFIX1 + "notpath"));
      }
    }

    // Combination of ns and au id that both exist, but have no artifacts
    // in common
    Pair<String, String> nsAu = nsAuMismatch();
    if (nsAu != null) {
      assertEmpty(repoClient.getArtifactsWithPrefix(nsAu.getLeft(),
          nsAu.getRight(),
          PREFIX1));
    }
  }

  public void testGetAllArtifactsAllVersions() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
        "Null AUID",
        () -> repoClient.getArtifactsAllVersions(null, null));
    assertThrowsMatch(IllegalArgumentException.class,
        "Null AUID",
        () -> repoClient.getArtifactsAllVersions(NS1, null));

    // Non-existent namespace & auid
    assertEmpty(repoClient.getArtifactsAllVersions(NO_NAMESPACE, NO_AUID));

    String anyColl = null;
    String anyAuid = null;
    // Compare with all URLs all version in each AU
    for (String ns : addedNamespaces()) {
      anyColl = ns;
      for (String auid : addedAuids()) {
        anyAuid = auid;
        ArtifactSpec.assertArtList(repoClient, orderedAllAu(ns, auid),
            repoClient.getArtifactsAllVersions(ns, auid));
      }
    }
    // Combination of ns and au id that both exist, but have no artifacts
    // in common
    Pair<String, String> nsAu = nsAuMismatch();
    if (nsAu != null) {
      assertEmpty(repoClient.getArtifactsAllVersions(nsAu.getLeft(),
          nsAu.getRight()));
    }
    if (anyColl != null && anyAuid != null) {
      assertEmpty(repoClient.getArtifactsAllVersions(anyColl,
          anyAuid + "_not"));
      assertEmpty(repoClient.getArtifactsAllVersions(anyColl + "_not",
          anyAuid));
    }
  }

  public void testGetAllArtifactsWithPrefixAllVersions() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
        "Null AUID or URL prefix",
        () -> repoClient.getArtifactsWithPrefixAllVersions(null, null, null));
    assertThrowsMatch(IllegalArgumentException.class,
        "prefix",
        () -> repoClient.getArtifactsWithPrefixAllVersions(NS1, AUID1, null));
    assertThrowsMatch(IllegalArgumentException.class,
        "Null AUID",
        () -> repoClient.getArtifactsWithPrefixAllVersions(NS1, null, PREFIX1));

    // Non-existent namespace & auid
    assertEmpty(repoClient.getArtifactsWithPrefixAllVersions(NO_NAMESPACE, NO_AUID, PREFIX1));
    // Compare with all URLs matching prefix in each AU
    for (String ns : addedNamespaces()) {
      for (String auid : addedAuids()) {
        ArtifactSpec.assertArtList(repoClient, (orderedAllAu(ns, auid)
                .filter(spec -> spec.getUrl().startsWith(PREFIX1))),
            repoClient.getArtifactsWithPrefixAllVersions(ns, auid, PREFIX1));
        assertEmpty(repoClient.getArtifactsWithPrefixAllVersions(ns, auid,
            PREFIX1 + "notpath"));
      }
    }

    // Combination of ns and au id that both exist, but have no artifacts
    // in common
    Pair<String, String> nsAu = nsAuMismatch();
    if (nsAu != null) {
      assertEmpty(repoClient.getArtifactsWithPrefixAllVersions(nsAu.getLeft(),
          nsAu.getRight(),
          PREFIX1));
    }
  }

  public void testGetArtifactAllVersions() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
        "Null AUID or URL",
        () -> repoClient.getArtifactsAllVersions(null, null, null));
    assertThrowsMatch(IllegalArgumentException.class,
        "URL",
        () -> repoClient.getArtifactsAllVersions(NS1, AUID1, null));
    assertThrowsMatch(IllegalArgumentException.class,
        "AUID",
        () -> repoClient.getArtifactsAllVersions(NS1, null, URL1));

    // Non-existent namespace, auid or url
    assertEmpty(repoClient.getArtifactsAllVersions(NO_NAMESPACE, AUID1, URL1));
    assertEmpty(repoClient.getArtifactsAllVersions(NS1, NO_AUID, URL1));
    assertEmpty(repoClient.getArtifactsAllVersions(NS1, AUID1, NO_URL));

    // For each ArtButVer in the repository, enumerate all its versions and
    // compare with expected
    Stream<ArtifactSpec> s =
        committedSpecStream().filter(distinctByKey(ArtifactSpec::artButVerKey));
    for (ArtifactSpec urlSpec : (Iterable<ArtifactSpec>) s::iterator) {
      ArtifactSpec.assertArtList(repoClient, orderedAllCommitted()
              .filter(spec -> spec.sameArtButVer(urlSpec)),
          repoClient.getArtifactsAllVersions(urlSpec.getNamespace(),
              urlSpec.getAuid(),
              urlSpec.getUrl()));
    }
  }

  public void testGetArtifactsWithUrlFromAllAus() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
        "Null URL",
        () -> repoClient.getArtifactsWithUrlFromAllAus(null, null, ArtifactVersions.ALL));
    assertThrowsMatch(IllegalArgumentException.class,
        "Null URL",
        () -> repoClient.getArtifactsWithUrlFromAllAus(NS1, null, ArtifactVersions.ALL));

    assertThrowsMatch(IllegalArgumentException.class,
        "Null URL",
        () -> repoClient.getArtifactsWithUrlFromAllAus(null, null, ArtifactVersions.LATEST));
    assertThrowsMatch(IllegalArgumentException.class,
        "Null URL",
        () -> repoClient.getArtifactsWithUrlFromAllAus(NS1, null, ArtifactVersions.LATEST));

    // Non-existent namespace or url
    assertEmpty(repoClient.getArtifactsWithUrlFromAllAus(NO_NAMESPACE, URL1, ArtifactVersions.ALL));
    assertEmpty(repoClient.getArtifactsWithUrlFromAllAus(NS1, NO_URL, ArtifactVersions.ALL));

    assertEmpty(repoClient.getArtifactsWithUrlFromAllAus(NO_NAMESPACE, URL1, ArtifactVersions.LATEST));
    assertEmpty(repoClient.getArtifactsWithUrlFromAllAus(NS1, NO_URL, ArtifactVersions.LATEST));

    // For each distinct URL in the specs, ask the repo for all artifacts
    // with that URL, check against the specs (sorted by (uri, auid,
    // version), to match ...AllAus())
    Stream<ArtifactSpec> s =
        committedSpecStream().filter(distinctByKey(ArtifactSpec::getUrl));
    for (ArtifactSpec urlSpec : (Iterable<ArtifactSpec>) s::iterator) {
      ArtifactSpec.assertArtList(repoClient,
          committedSpecStream()
              .sorted(ArtifactSpec.ART_SPEC_COMPARATOR_BY_URL)
              .filter(spec -> spec.getUrl().equals(urlSpec.getUrl()))
              .filter(spec -> spec.getNamespace().equals(urlSpec.getNamespace())),
          repoClient.getArtifactsWithUrlFromAllAus(urlSpec.getNamespace(),
              urlSpec.getUrl(), ArtifactVersions.ALL));

      ArtifactSpec.assertArtList(repoClient,
          committedSpecStream()
              .sorted(ArtifactSpec.ART_SPEC_COMPARATOR_BY_URL)
              .filter(spec -> spec.getUrl().equals(urlSpec.getUrl()))
              .filter(spec -> spec.getNamespace().equals(urlSpec.getNamespace()))
              .collect(Collectors.groupingBy(
                  spec -> new ArtifactIdentifier.ArtifactStem(spec.getNamespace(), spec.getAuid(), spec.getUrl()),
                  Collectors.maxBy(Comparator.comparingInt(ArtifactSpec::getVersion))))
              .values()
              .stream()
              .filter(Optional::isPresent)
              .map(Optional::get)
              .sorted(
                  // ArtifactSpec equivalent of ArtifactComparators.BY_URI_BY_AUID_BY_DECREASING_VERSION
                  Comparator.comparing(ArtifactSpec::getUrl, PreOrderComparator.INSTANCE)
                      .thenComparing(ArtifactSpec::getAuid)
                      .thenComparing(Comparator.comparingInt(ArtifactSpec::getVersion).reversed())
              ),
          repoClient.getArtifactsWithUrlFromAllAus(urlSpec.getNamespace(),
              urlSpec.getUrl(), ArtifactVersions.LATEST));
    }
  }

  public void testGetArtifactsWithUrlPrefixFromAllAus() throws IOException {
    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
        "Null URL prefix",
        () -> repoClient.getArtifactsWithUrlPrefixFromAllAus(null, null, ArtifactVersions.ALL));
    assertThrowsMatch(IllegalArgumentException.class,
        "Null URL prefix",
        () -> repoClient.getArtifactsWithUrlPrefixFromAllAus(NS1, null, ArtifactVersions.ALL));

    // Illegal args
    assertThrowsMatch(IllegalArgumentException.class,
        "Null URL prefix",
        () -> repoClient.getArtifactsWithUrlPrefixFromAllAus(null, null, ArtifactVersions.LATEST));
    assertThrowsMatch(IllegalArgumentException.class,
        "Null URL prefix",
        () -> repoClient.getArtifactsWithUrlPrefixFromAllAus(NS1, null, ArtifactVersions.LATEST));

    // Non-existent namespace or url
    assertEmpty(repoClient.getArtifactsWithUrlPrefixFromAllAus(NO_NAMESPACE, URL1, ArtifactVersions.ALL));
    assertEmpty(repoClient.getArtifactsWithUrlPrefixFromAllAus(NS1, NO_URL, ArtifactVersions.ALL));

    assertEmpty(repoClient.getArtifactsWithUrlPrefixFromAllAus(NO_NAMESPACE, URL1, ArtifactVersions.LATEST));
    assertEmpty(repoClient.getArtifactsWithUrlPrefixFromAllAus(NS1, NO_URL, ArtifactVersions.LATEST));

    // Get all the Artifacts beginning with URL_PREFIX, check agains the specs
    // (sorted by (uri, auid, version), to match ...AllAus())
    ArtifactSpec.assertArtList(repoClient,
        committedSpecStream()
            .sorted(ArtifactSpec.ART_SPEC_COMPARATOR_BY_URL)
            .filter(spec -> spec.getUrl().startsWith(URL_PREFIX))
            .filter(spec -> spec.getNamespace().equals(NS1)),
        repoClient.getArtifactsWithUrlPrefixFromAllAus(NS1, URL_PREFIX, ArtifactVersions.ALL));

    ArtifactSpec.assertArtList(repoClient,
        committedSpecStream()
            .sorted(ArtifactSpec.ART_SPEC_COMPARATOR_BY_URL)
            .filter(spec -> spec.getUrl().startsWith(URL_PREFIX))
            .filter(spec -> spec.getNamespace().equals(NS1))
            .collect(Collectors.groupingBy(
                spec -> new ArtifactIdentifier.ArtifactStem(spec.getNamespace(), spec.getAuid(), spec.getUrl()),
                Collectors.maxBy(Comparator.comparingInt(ArtifactSpec::getVersion))))
            .values()
            .stream()
            .filter(Optional::isPresent)
            .map(Optional::get)
            .sorted(
                // ArtifactSpec equivalent of ArtifactComparators.BY_URI_BY_AUID_BY_DECREASING_VERSION
                Comparator.comparing(ArtifactSpec::getUrl, PreOrderComparator.INSTANCE)
                    .thenComparing(ArtifactSpec::getAuid)
                    .thenComparing(Comparator.comparingInt(ArtifactSpec::getVersion).reversed())
            ),
        repoClient.getArtifactsWithUrlPrefixFromAllAus(NS1, URL_PREFIX, ArtifactVersions.LATEST));

    // Same with empty prefix
    ArtifactSpec.assertArtList(repoClient,
        committedSpecStream()
            .sorted(ArtifactSpec.ART_SPEC_COMPARATOR_BY_URL)
            .filter(spec -> spec.getNamespace().equals(NS1)),
        repoClient.getArtifactsWithUrlPrefixFromAllAus(NS1, "", ArtifactVersions.ALL));

    ArtifactSpec.assertArtList(repoClient,
        committedSpecStream()
            .sorted(ArtifactSpec.ART_SPEC_COMPARATOR_BY_URL)
            .filter(spec -> spec.getNamespace().equals(NS1))
            .collect(Collectors.groupingBy(
                spec -> new ArtifactIdentifier.ArtifactStem(spec.getNamespace(), spec.getAuid(), spec.getUrl()),
                Collectors.maxBy(Comparator.comparingInt(ArtifactSpec::getVersion))))
            .values()
            .stream()
            .filter(Optional::isPresent)
            .map(Optional::get)
            .sorted(
                // ArtifactSpec equivalent of ArtifactComparators.BY_URI_BY_AUID_BY_DECREASING_VERSION
                Comparator.comparing(ArtifactSpec::getUrl, PreOrderComparator.INSTANCE)
                    .thenComparing(ArtifactSpec::getAuid)
                    .thenComparing(Comparator.comparingInt(ArtifactSpec::getVersion).reversed())
            ),
        repoClient.getArtifactsWithUrlPrefixFromAllAus(NS1, "", ArtifactVersions.LATEST));
  }

  public void testGetAuIds() throws IOException {
    // Non-existent namespace
    assertEmpty(repoClient.getAuIds(NO_NAMESPACE));

    // Compare with expected auid list for each namespace
    for (String coll : addedNamespaces()) {
      Iterator<String> expAuids =
          orderedAllNs(coll)
              .map(ArtifactSpec::getAuid)
              .distinct()
              .iterator();
      assertEquals(IteratorUtils.toList(expAuids),
          IteratorUtils.toList(repoClient.getAuIds(coll).iterator()));
    }

    // Try getAuIds() on namespaces that have no committed artifacts
    for (String namespace : CollectionUtils.subtract(addedNamespaces(),
        addedCommittedNamespaces())) {
      assertEmpty(repoClient.getAuIds(namespace));
    }
  }

  public void testGetNamespaces() throws IOException {
    Iterator<String> expColl =
        orderedAllCommitted()
            .map(ArtifactSpec::getNamespace)
            .distinct()
            .iterator();
    assertEquals(IteratorUtils.toList(expColl),
        IteratorUtils.toList(repoClient.getNamespaces().iterator()));
  }

  // SCENARIOS

  protected enum StdVariants {
    empty, commit1, uncommit1, url3, url3unc, disjoint,
    grid3x3x3, grid3x3x3x3,
  }

  /**
   * Return a list of ArtifactSpecs for the initial conditions for the named
   * variant
   */
  public List<ArtifactSpec> getVariantSpecs(String variant) throws IOException {
    List<ArtifactSpec> res = new ArrayList<ArtifactSpec>();
    switch (variant) {
      case "no_variant":
        // Not a variant test
        break;
      case "empty":
        // Empty repository
        break;
      case "commit1":
        // One committed artifact
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL1).toCommit(true));
        break;
      case "uncommit1":
        // One uncommitted artifact
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL1));
        break;
      case "url3":
        // Three committed versions
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL1).toCommit(true));
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL1).toCommit(true));
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL1).toCommit(true));
        break;
      case "url3unc":
        // Mix of committed and uncommitted, two URLs
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL1).toCommit(true));
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL1));
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL1).toCommit(true));

        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL2).toCommit(true));
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL2).toCommit(true));
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL2));
        break;
      case "disjoint":
        // Different URLs in different namespaces and AUs
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL1).toCommit(true));
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL1));
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL1).toCommit(true));

        res.add(ArtifactSpec.forNsAuUrl(NS2, AUID2, URL2).toCommit(true));
        res.add(ArtifactSpec.forNsAuUrl(NS2, AUID2, URL2).toCommit(true));
        res.add(ArtifactSpec.forNsAuUrl(NS2, AUID2, URL2));
        break;
      case "overlap":
        // Same URLs in different namespaces and AUs
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL1).toCommit(true));
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL1));
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL1).toCommit(true));
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL2).toCommit(true));
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL2));
        res.add(ArtifactSpec.forNsAuUrl(NS1, AUID1, URL2).toCommit(true));

        res.add(ArtifactSpec.forNsAuUrl(NS2, AUID2, URL1).toCommit(true));
        res.add(ArtifactSpec.forNsAuUrl(NS2, AUID2, URL1).toCommit(true));
        res.add(ArtifactSpec.forNsAuUrl(NS2, AUID2, URL1));
        res.add(ArtifactSpec.forNsAuUrl(NS2, AUID2, URL2).toCommit(true));
        res.add(ArtifactSpec.forNsAuUrl(NS2, AUID2, URL2).toCommit(true));
        res.add(ArtifactSpec.forNsAuUrl(NS2, AUID2, URL2));
        break;
      case "grid3x3x3":
        // Combinatorics of namespace, AU, URL
      {
        boolean toCommit = false;
        for (String ns : NAMESPACES) {
          for (String auid : AUIDS) {
            for (String url : URLS) {
              res.add(ArtifactSpec.forNsAuUrl(ns, auid, url).toCommit(toCommit));
              toCommit = !toCommit;
            }
          }
        }
      }
      break;
      case "grid3x3x3x3":
        // Combinatorics of namespace, AU, URL w/ multiple versions
      {
        boolean toCommit = false;
        for (int ix = 1; ix <= 3; ix++) {
          for (String ns : NAMESPACES) {
            for (String auid : AUIDS) {
              for (String url : URLS) {
                res.add(ArtifactSpec.forNsAuUrl(ns, auid, url).toCommit(toCommit));
                toCommit = !toCommit;
              }
            }
          }
        }
      }
      break;
      case "unicode":
        res.add(ArtifactSpec.forNsAuUrl("c", "a", "111").thenCommit());
        res.add(ArtifactSpec.forNsAuUrl("c", "a", "ZZZ").thenCommit());
        res.add(ArtifactSpec.forNsAuUrl("c", "a", "zzz").thenCommit());
        res.add(ArtifactSpec.forNsAuUrl("c", "a", "\u03BA\u1F79\u03C3\u03BC\u03B5").thenCommit());
        res.add(ArtifactSpec.forNsAuUrl("c", "a", "Heiz\u00F6lr\u00FCcksto\u00DFabd\u00E4mpfung").thenCommit());
        break;
      default:
        fail("getVariantSpecs called with unknown variant name: " + variant);
    }
    return res;
  }

  // utilities


  // Add Artifacts to the repository as specified by the named scenario
  void instantiateScanario(String name) throws IOException {
    log.info("Adding scenario: " + name);
    instantiateScanario(getVariantSpecs(name));
  }

  // Add Artifacts to the repository as specified by the ArtifactSpecs
  void instantiateScanario(List<ArtifactSpec> scenario) throws IOException {
    for (ArtifactSpec spec : scenario) {
      Artifact art = addUncommitted(spec);
      if (spec.isToCommit()) {
        commit(spec, art);
      }
    }
  }

  void logAdded() {
    for (ArtifactSpec spec : addedSpecs) {
      log.info("spec: " + spec);
    }
  }

  long expectedVersions(ArtifactSpec spec) {
    return addedSpecs.stream()
        .filter(s -> spec.sameArtButVer(s))
        .count();
  }

  List<String> addedAuids() {
    return addedSpecs.stream()
        .map(ArtifactSpec::getAuid)
        .distinct()
        .collect(Collectors.toList());
  }

  List<String> addedCommittedAuids() {
    return addedSpecs.stream()
        .filter(spec -> spec.isCommitted())
        .map(ArtifactSpec::getAuid)
        .distinct()
        .collect(Collectors.toList());
  }

  List<String> addedCommittedUrls() {
    return addedSpecs.stream()
        .filter(spec -> spec.isCommitted())
        .map(ArtifactSpec::getUrl)
        .distinct()
        .collect(Collectors.toList());
  }

  List<String> addedNamespaces() {
    return addedSpecs.stream()
        .map(ArtifactSpec::getNamespace)
        .distinct()
        .collect(Collectors.toList());
  }

  List<String> addedCommittedNamespaces() {
    return addedSpecs.stream()
        .filter(spec -> spec.isCommitted())
        .map(ArtifactSpec::getNamespace)
        .distinct()
        .collect(Collectors.toList());
  }

  Stream<String> namespacesOf(Stream<ArtifactSpec> specStream) {
    return specStream
        .map(ArtifactSpec::getNamespace)
        .distinct();
  }

  Stream<String> auidsOf(Stream<ArtifactSpec> specStream, String namespace) {
    return specStream
        .filter(s -> s.getNamespace().equals(namespace))
        .map(ArtifactSpec::getAuid)
        .distinct();
  }

  Stream<ArtifactSpec> addedSpecStream() {
    return addedSpecs.stream();
  }

  Stream<ArtifactSpec> committedSpecStream() {
    return addedSpecs.stream()
        .filter(spec -> spec.isCommitted());
  }

  Stream<ArtifactSpec> uncommittedSpecStream() {
    return addedSpecs.stream()
        .filter(spec -> !spec.isCommitted());
  }

  Stream<ArtifactSpec> orderedAllCommitted() {
    return committedSpecStream()
        .sorted();
  }

  public static <T> Predicate<T>
  distinctByKey(Function<? super T, Object> keyExtractor) {
    Set<Object> seen = new HashSet<>();
    return t -> seen.add(keyExtractor.apply(t));
  }

  Stream<ArtifactSpec> orderedAllNs(String ns) {
    return committedSpecStream()
        .filter(s -> s.getNamespace().equals(ns))
        .sorted();
  }

  Stream<ArtifactSpec> orderedAllAu(String ns, String auid) {
    return committedSpecStream()
        .filter(s -> s.getNamespace().equals(ns))
        .filter(s -> s.getAuid().equals(auid))
        .sorted();
  }

  Stream<ArtifactSpec> orderedAllUrl(String ns, String auid, String url) {
    return committedSpecStream()
        .filter(s -> s.getNamespace().equals(ns))
        .filter(s -> s.getAuid().equals(auid))
        .filter(s -> s.getUrl().equals(url))
        .sorted();
  }

  ArtifactSpec anyCommittedSpec() {
    return committedSpecStream().findAny().orElse(null);
  }

  ArtifactSpec anyUncommittedSpec() {
    return uncommittedSpecStream().findAny().orElse(null);
  }

  ArtifactSpec anyUncommittedSpecButVer() {
    return uncommittedSpecStream()
        .filter(spec -> !highestCommittedVerSpec.containsKey(spec.artButVerKey()))
        .findAny().orElse(null);
  }


  // Find a namespace and an au that each have artifacts, but don't have
  // any artifacts in common
  Pair<String, String> nsAuMismatch() {
    Set<Pair<String, String>> set = new HashSet<Pair<String, String>>();
    for (String ns : addedCommittedNamespaces()) {
      for (String auid : addedCommittedAuids()) {
        set.add(new ImmutablePair<String, String>(ns, auid));
      }
    }
    committedSpecStream()
        .forEach(spec -> {
          set.remove(
              new ImmutablePair<String, String>(spec.getNamespace(),
                  spec.getAuid()));
        });
    if (set.isEmpty()) {
      return null;
    } else {
      Pair<String, String> res = set.iterator().next();
      log.info("Found ns au mismatch: " +
          res.getLeft() + ", " + res.getRight());
      logAdded();
      return res;
    }
  }

  // Return the highest version ArtifactSpec with same ArtButVer
  ArtifactSpec highestVer(ArtifactSpec likeSpec, Stream<ArtifactSpec> stream) {
    return stream
        .filter(spec -> spec.sameArtButVer(likeSpec))
        .max(Comparator.comparingInt(ArtifactSpec::getVersion))
        .orElse(null);
  }

  // Delete ArtifactSpec from record of what we've added to the repository,
  // adjust highest version maps accordingly
  protected void delFromAll(ArtifactSpec spec) {
    if (!addedSpecs.remove(spec)) {
      fail("Wasn't removed from processedSpecs: " + spec);
    }
    String key = spec.artButVerKey();
    if (highestVerSpec.get(key) == spec) {
      ArtifactSpec newHigh = highestVer(spec, addedSpecStream());
      log.info("newHigh: " + newHigh);
      highestVerSpec.put(key, newHigh);
    }
    if (highestCommittedVerSpec.get(key) == spec) {
      ArtifactSpec newCommHigh = highestVer(spec, committedSpecStream());
      log.info("newCommHigh: " + newCommHigh);
      highestCommittedVerSpec.put(key, newCommHigh);
    }
  }

  Artifact getArtifact(LockssRepository repository, ArtifactSpec spec,
                       boolean includeUncommitted) throws IOException {
    log.info(String.format("getArtifact(%s, %s, %s)",
        spec.getNamespace(),
        spec.getAuid(),
        spec.getUrl(),
        includeUncommitted));
    if (spec.hasVersion()) {
      return repository.getArtifactVersion(spec.getNamespace(),
          spec.getAuid(),
          spec.getUrl(),
          spec.getVersion(),
          includeUncommitted);
    } else {
      return repository.getArtifact(spec.getNamespace(),
          spec.getAuid(),
          spec.getUrl());
    }
  }

  Artifact getArtifactVersion(LockssRepository repository, ArtifactSpec spec,
                              int ver, boolean includeUncommitted)
      throws IOException {
    log.info(String.format("getArtifactVersion(%s, %s, %s, %d)",
        spec.getNamespace(),
        spec.getAuid(),
        spec.getUrl(),
        ver,
        includeUncommitted));
    return repository.getArtifactVersion(spec.getNamespace(),
        spec.getAuid(),
        spec.getUrl(),
        ver,
        includeUncommitted);
  }

  Artifact addUncommitted(ArtifactSpec spec) throws IOException {
    if (!spec.hasContent()) {
      spec.generateContent();
    }
    log.info("adding: " + spec);

    ArtifactData ad = spec.getArtifactData();
    Artifact newArt = repoClient.addArtifact(ad);
    assertNotNull(newArt);

    spec.assertArtifact(repoClient, newArt);

    long expVers = expectedVersions(spec);
    assertEquals("version of " + newArt,
        expVers + 1, (int) newArt.getVersion());
    if (spec.getExpVer() >= 0) {
      throw new IllegalStateException("addUncommitted() must be called with unused ArtifactSpec");
    }

    String newArtUuid = newArt.getUuid();
    assertNotNull(newArtUuid);
    assertFalse(newArt.getCommitted());
    assertNotNull(repoClient.getArtifactData(newArt));

    Artifact oldArt = getArtifact(repoClient, spec, false);
    if (expVers == 0) {
      // this test valid only when no other versions exist ArtifactSpec
      assertNull(oldArt);
    }

    if (spec.hasVersion()) {
      assertEquals(newArt, getArtifact(repoClient, spec, true));
    }

    assertNull(repoClient.getArtifactVersion(newArt.getNamespace(),
        newArt.getAuid(), newArt.getUri(), newArt.getVersion(), false));

    log.debug("newArt = " + newArt);
    Artifact newArt2 = repoClient.getArtifactVersion(newArt.getNamespace(),
        newArt.getAuid(), newArt.getUri(), newArt.getVersion(), true);
    log.debug("newArt2 = " + newArt2);
    assertEquals(newArt, newArt2);

    spec.setVersion(newArt.getVersion());
    spec.setArtifactUuid(newArtUuid);

    addedSpecs.add(spec);
    // Remember the highest version of this URL we've added
    ArtifactSpec maxVerSpec = highestVerSpec.get(spec.artButVerKey());
    if (maxVerSpec == null || maxVerSpec.getVersion() < spec.getVersion()) {
      highestVerSpec.put(spec.artButVerKey(), spec);
    }
    return newArt;
  }

  Artifact commit(ArtifactSpec spec, Artifact art) throws IOException {
    Artifact uncommittedArt = getArtifact(repoClient, spec, true);
    assertNotNull(uncommittedArt);
    assertFalse(uncommittedArt.getCommitted());

    String artUuid = art.getUuid();
    log.info("committing: " + art);
    Artifact commArt = repoClient.commitArtifact(spec.getNamespace(), artUuid);
    assertNotNull(commArt);

    if (spec.getExpVer() > 0) {
      assertEquals(spec.getExpVer(), (int) commArt.getVersion());
    }
    spec.setCommitted(true);
    // Remember the highest version of this URL we've committed
    ArtifactSpec maxVerSpec = highestCommittedVerSpec.get(spec.artButVerKey());
    if (maxVerSpec == null || maxVerSpec.getVersion() < spec.getVersion()) {
      highestCommittedVerSpec.put(spec.artButVerKey(), spec);
    }
    assertTrue(commArt.getCommitted());

    spec.assertArtifact(repoClient, commArt);

    Artifact newArt = getArtifact(repoClient, spec, false);
    assertNotNull(newArt);
    assertTrue(newArt.getCommitted());
    assertNotNull(repoClient.getArtifactData(newArt));
    // Get the same artifact when uncommitted may be included.
    Artifact newArt2 = getArtifact(repoClient, spec, true);
    assertTrue(newArt.equalsExceptStorageUrl(newArt2));
    return newArt;
  }

  // These should all cause addArtifact to throw NPE 
  protected ArtifactData[] nullPointerArtData = {
      new ArtifactData(null, null, null),
      new ArtifactData(null, null, STATUS_LINE_OK),
      new ArtifactData(null, stringInputStream(""), null),
      new ArtifactData(null, stringInputStream(""), STATUS_LINE_OK),
      new ArtifactData(HEADERS1, null, null),
      new ArtifactData(HEADERS1, null, STATUS_LINE_OK),
      new ArtifactData(HEADERS1, stringInputStream(""), null),
  };

  // These describe artifacts that getArtifact() should never find
  protected ArtifactSpec[] neverFoundArtifactSpecs = {
      ArtifactSpec.forNsAuUrl(NO_NAMESPACE, AUID1, URL1),
      ArtifactSpec.forNsAuUrl(NS1, NO_AUID, URL1),
      ArtifactSpec.forNsAuUrl(NS1, AUID1, NO_URL),
  };

  /**
   * Return list of ArtifactSpecs that shouldn't be found in the current
   * repository
   */
  protected List<ArtifactSpec> notFoundArtifactSpecs() {
    List<ArtifactSpec> res = new ArrayList<ArtifactSpec>();
    // Always include some that should never be found
    Collections.addAll(res, neverFoundArtifactSpecs);

    // Include an uncommitted artifact, if any
    ArtifactSpec uncSpec = anyUncommittedSpecButVer();
    if (uncSpec != null) {
      log.info("adding an uncommitted spec: " + uncSpec);
      res.add(uncSpec);
    }

    // If there's at least one committed artifact ...
    ArtifactSpec commSpec = anyCommittedSpec();
    if (commSpec != null) {
      // include variants of it with non-existent namespace, au, etc.
      res.add(commSpec.copy().setNamespace("NO_" + commSpec.getNamespace()));
      res.add(commSpec.copy().setAuid("NO_" + commSpec.getAuid()));
      res.add(commSpec.copy().setUrl("NO_" + commSpec.getUrl()));

      // and with existing but different namespace, au
      diff_ns:
      for (ArtifactSpec auUrl : committedSpecStream()
          .filter(distinctByKey(s -> s.getUrl() + "|" + s.getAuid()))
          .collect(Collectors.toList())) {
        for (String ns : addedCommittedNamespaces()) {
          ArtifactSpec a = auUrl.copy().setNamespace(ns);
          if (!highestCommittedVerSpec.containsKey(a.artButVerKey())) {
            res.add(a);
            break diff_ns;
          }
        }
      }
      diff_au:
      for (ArtifactSpec auUrl : committedSpecStream()
          .filter(distinctByKey(s -> s.getUrl() + "|" + s.getNamespace()))
          .collect(Collectors.toList())) {
        for (String auid : addedCommittedAuids()) {
          ArtifactSpec a = auUrl.copy().setAuid(auid);
          if (!highestCommittedVerSpec.containsKey(a.artButVerKey())) {
            res.add(a);
            break diff_au;
          }
        }
      }
      diff_url:
      for (ArtifactSpec auUrl : committedSpecStream()
          .filter(distinctByKey(s -> s.getAuid() + "|" + s.getNamespace()))
          .collect(Collectors.toList())) {
        for (String url : addedCommittedUrls()) {
          ArtifactSpec a = auUrl.copy().setUrl(url);
          if (!highestCommittedVerSpec.containsKey(a.artButVerKey())) {
            res.add(a);
            break diff_url;
          }
        }
      }

      // and with correct ns, au, url but non-existent version
      res.add(commSpec.copy().setVersion(0));
      res.add(commSpec.copy().setVersion(1000));
    }

    return res;
  }

  /** Wait until the artifact has been copied to permanent storage */
  private Artifact waitCopied(ArtifactSpec spec)
      throws IOException, InterruptedException {
    return waitCopied(spec.getNamespace(), spec.getAuid(), spec.getUrl());
  }

  /** Wait until the artifact has been copied to permanent storage */
  private Artifact waitCopied(String namespace, String auId, String url)
      throws IOException, InterruptedException {
    while (true) {
      Artifact artifact = repoClient.getArtifact(namespace, auId, url);
      if (!artifact.getStorageUrl().contains("tmp/warc")) {
        return artifact;
      }
      Thread.sleep(100);
    }
  }

  InputStream stringInputStream(String str) {
    return IOUtils.toInputStream(str, Charset.defaultCharset());
  }

}
