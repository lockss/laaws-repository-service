/*

Copyright (c) 2018 Board of Trustees of Leland Stanford Jr. University,
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
package org.lockss.laaws.rs.io.index;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.lockss.laaws.rs.io.storage.local.WarcRepositoryArtifactMetadata;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.ArtifactIndexData;
import org.lockss.test.LockssTestCase4;

/**
 * Test class for {@code org.lockss.laaws.rs.io.index.VolatileArtifactIndex}
 */
public class TestVolatileArtifactIndex extends LockssTestCase4 {

  private ArtifactIdentifier aid1;
  private UUID uuid;
  private ArtifactIdentifier aid2;
  private WarcRepositoryArtifactMetadata md1;
  private WarcRepositoryArtifactMetadata md2;
  private Artifact artifact1;
  private Artifact artifact2;
  private VolatileArtifactIndex index;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    aid1 = new ArtifactIdentifier("id1", "coll1", "auid1", "uri1", "v1");
    uuid = UUID.randomUUID();
    aid2 =
	new ArtifactIdentifier(uuid.toString(), "coll2", "auid2", "uri2", "v2");

    md1 = new WarcRepositoryArtifactMetadata(aid1, "wfp1", 1, false, false);
    md2 = new WarcRepositoryArtifactMetadata(aid2, "wfp2", 2, true, false);

    artifact1 = new Artifact(aid1, null, null, null, md1);
    artifact2 = new Artifact(aid2, null, null, null, md2);

    index = new VolatileArtifactIndex();
  }

  /*
   * Test methods.
   */
  @Test
  public void testIndexArtifact() {
    String expectedMessage = "Null artifact";

    try {
      index.indexArtifact(null);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    expectedMessage = "Artifact has null identifier";

    try {
      index.indexArtifact(new Artifact(null, null, null, null, null));
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    expectedMessage = "Artifact has null repository metadata";

    try {
      index.indexArtifact(new Artifact(aid1, null, null, null, null));
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    ArtifactIndexData aidata = index.indexArtifact(artifact1);

    assertEquals("id1", aidata.getId());
    assertEquals("coll1", aidata.getCollection());
    assertEquals("auid1", aidata.getAuid());
    assertEquals("uri1", aidata.getUri());
    assertEquals("v1", aidata.getVersion());
    assertEquals("id1", aidata.getWarcRecordId());
    assertEquals("wfp1", aidata.getWarcFilePath());
    assertEquals(1, aidata.getWarcRecordOffset());
    assertEquals(false, aidata.getCommitted());
    assertEquals(aidata, index.getArtifactIndexData("id1"));

    aidata = index.indexArtifact(artifact2);

    assertEquals(uuid.toString(), aidata.getId());
    assertEquals("coll2", aidata.getCollection());
    assertEquals("auid2", aidata.getAuid());
    assertEquals("uri2", aidata.getUri());
    assertEquals("v2", aidata.getVersion());
    assertEquals(uuid.toString(), aidata.getWarcRecordId());
    assertEquals("wfp2", aidata.getWarcFilePath());
    assertEquals(2, aidata.getWarcRecordOffset());
    assertEquals(false, aidata.getCommitted());
    assertEquals(aidata, index.getArtifactIndexData(uuid.toString()));

    aidata = index.indexArtifact(artifact2);

    assertEquals(uuid.toString(), aidata.getId());
    assertEquals("coll2", aidata.getCollection());
    assertEquals("auid2", aidata.getAuid());
    assertEquals("uri2", aidata.getUri());
    assertEquals("v2", aidata.getVersion());
    assertEquals(uuid.toString(), aidata.getWarcRecordId());
    assertEquals("wfp2", aidata.getWarcFilePath());
    assertEquals(2, aidata.getWarcRecordOffset());
    assertEquals(false, aidata.getCommitted());
    assertEquals(aidata, index.getArtifactIndexData(uuid.toString()));
  }

  @Test
  public void testGetArtifactIndexData() {
    String expectedMessage = null;

    try {
      String stringId = null;
      expectedMessage = "Null or empty identifier";
      index.getArtifactIndexData(stringId);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    try {
      UUID nullIdUuid = null;
      expectedMessage = "Null UUID";
      index.getArtifactIndexData(nullIdUuid);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    assertNull(index.getArtifactIndexData("id1"));
    ArtifactIndexData aidata1 = index.indexArtifact(artifact1);
    assertEquals(aidata1, index.getArtifactIndexData("id1"));

    assertNull(index.getArtifactIndexData(uuid));
    ArtifactIndexData aidata2 = index.indexArtifact(artifact2);
    assertEquals(aidata2, index.getArtifactIndexData(uuid));

    aidata1 = index.indexArtifact(artifact1);
    assertEquals(aidata1, index.getArtifactIndexData("id1"));
  }

  @Test
  public void testCommitArtifact() {
    String expectedMessage = null;

    try {
      String stringId = null;
      expectedMessage = "Null or empty identifier";
      index.commitArtifact(stringId);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    try {
      UUID uuidId = null;
      expectedMessage = "Null UUID";
      index.commitArtifact(uuidId);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    assertNull(index.commitArtifact("unknown"));
    assertNull(index.commitArtifact(UUID.randomUUID()));

    index.indexArtifact(artifact1);
    index.indexArtifact(artifact2);

    assertFalse(index.getArtifactIndexData("id1").getCommitted());
    assertFalse(index.getArtifactIndexData(uuid).getCommitted());

    index.commitArtifact("id1");

    assertTrue(index.getArtifactIndexData("id1").getCommitted());
    assertFalse(index.getArtifactIndexData(uuid).getCommitted());

    index.commitArtifact(uuid);

    assertTrue(index.getArtifactIndexData("id1").getCommitted());
    assertTrue(index.getArtifactIndexData(uuid).getCommitted());

    index.commitArtifact("id1");

    assertTrue(index.getArtifactIndexData("id1").getCommitted());
    assertTrue(index.getArtifactIndexData(uuid).getCommitted());
  }

  @Test
  public void testDeleteArtifact() {
    String expectedMessage = null;

    try {
      String stringId = null;
      expectedMessage = "Null or empty identifier";
      index.deleteArtifact(stringId);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    try {
      UUID uuidId = null;
      expectedMessage = "Null UUID";
      index.deleteArtifact(uuidId);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    assertFalse(index.deleteArtifact("unknown"));
    assertFalse(index.deleteArtifact(UUID.randomUUID()));

    assertNull(index.getArtifactIndexData("id1"));
    assertNull(index.getArtifactIndexData(uuid.toString()));

    index.indexArtifact(artifact1);

    assertEquals("id1", index.getArtifactIndexData("id1").getId());
    assertNull(index.getArtifactIndexData(uuid.toString()));

    index.indexArtifact(artifact2);

    assertEquals("id1", index.getArtifactIndexData("id1").getId());
    assertEquals(uuid.toString(),
	index.getArtifactIndexData(uuid.toString()).getId());

    assertTrue(index.deleteArtifact("id1"));

    assertNull(index.getArtifactIndexData("id1"));
    assertEquals(uuid.toString(),
	index.getArtifactIndexData(uuid.toString()).getId());

    assertTrue(index.deleteArtifact(uuid));

    assertNull(index.getArtifactIndexData("id1"));
    assertNull(index.getArtifactIndexData(uuid.toString()));

    assertFalse(index.deleteArtifact(uuid));

    assertNull(index.getArtifactIndexData("id1"));
    assertNull(index.getArtifactIndexData(uuid.toString()));
  }

  @Test
  public void testArtifactExists() {
    String expectedMessage = "Null or empty identifier";

    try {
      String stringId = null;
      index.artifactExists(stringId);
      fail("Should have thrown IllegalArgumentException(" + expectedMessage
	  + ")");
    } catch (IllegalArgumentException iae) {
      assertEquals(expectedMessage, iae.getMessage());
    }

    assertFalse(index.artifactExists("unknown"));

    assertFalse(index.artifactExists("id1"));
    assertFalse(index.artifactExists(uuid.toString()));

    index.indexArtifact(artifact1);

    assertTrue(index.artifactExists("id1"));
    assertFalse(index.artifactExists(uuid.toString()));

    index.indexArtifact(artifact2);

    assertTrue(index.artifactExists("id1"));
    assertTrue(index.artifactExists(uuid.toString()));
  }

  @Test
  public void testGetCollectionIds() {
    assertFalse(index.getCollectionIds().hasNext());

    index.indexArtifact(artifact1);
    assertFalse(index.getCollectionIds().hasNext());

    index.commitArtifact("id1");

    Iterator<String> iter = index.getCollectionIds();
    assertTrue(iter.hasNext());
    assertEquals("coll1", iter.next());
    assertFalse(iter.hasNext());

    index.indexArtifact(artifact2);

    iter = index.getCollectionIds();
    assertTrue(iter.hasNext());
    assertEquals("coll1", iter.next());
    assertFalse(iter.hasNext());
    
    index.commitArtifact(uuid.toString());

    iter = index.getCollectionIds();
    assertTrue(iter.hasNext());

    Set<String> collections = new HashSet<>();
    collections.add(iter.next());
    assertTrue(iter.hasNext());
    collections.add(iter.next());
    assertFalse(iter.hasNext());
    assertEquals(2, collections.size());
    assertTrue(collections.contains("coll1"));
    assertTrue(collections.contains("coll2"));
  }

  @Test
  public void testGetAus() {
    assertTrue(index.getAus(null).isEmpty());
    assertTrue(index.getAus("coll1").isEmpty());

    index.indexArtifact(artifact1);
    assertTrue(index.getAus("coll1").isEmpty());

    index.commitArtifact("id1");
    assertFalse(index.getAus("coll1").isEmpty());

    List<ArtifactIndexData> aids = index.getAus("coll1").get("auid1");
    assertEquals(1, aids.size());
    assertEquals("id1", aids.get(0).getId());

    assertTrue(index.getAus("coll2").isEmpty());

    index.indexArtifact(artifact2);
    assertTrue(index.getAus("coll2").isEmpty());

    index.commitArtifact(uuid.toString());
    assertFalse(index.getAus("coll2").isEmpty());

    aids = index.getAus("coll2").get("auid2");
    assertEquals(1, aids.size());
    assertEquals(uuid.toString(), aids.get(0).getId());
  }

  @Test
  public void testGetArtifactsInAU() {
    assertFalse(index.getArtifactsInAU(null, null).hasNext());
    assertFalse(index.getArtifactsInAU("coll1", null).hasNext());
    assertFalse(index.getArtifactsInAU("coll1", "auid1").hasNext());

    index.indexArtifact(artifact1);
    assertFalse(index.getArtifactsInAU("coll1", "auid1").hasNext());

    index.commitArtifact("id1");

    Iterator<ArtifactIndexData> iter = index.getArtifactsInAU("coll1", "auid1");
    assertTrue(iter.hasNext());
    ArtifactIndexData aid = iter.next();
    assertEquals("id1", aid.getId());
    assertEquals("coll1", aid.getCollection());
    assertEquals("auid1", aid.getAuid());

    assertFalse(index.getArtifactsInAU("coll2", null).hasNext());
    assertFalse(index.getArtifactsInAU("coll2", "auid1").hasNext());
    assertFalse(index.getArtifactsInAU("coll2", "auid2").hasNext());

    index.indexArtifact(artifact2);
    assertFalse(index.getArtifactsInAU("coll2", "auid1").hasNext());
    assertFalse(index.getArtifactsInAU("coll2", "auid2").hasNext());

    index.commitArtifact(uuid.toString());
    assertFalse(index.getArtifactsInAU("coll2", "auid1").hasNext());

    iter = index.getArtifactsInAU("coll2", "auid2");
    assertTrue(iter.hasNext());
    aid = iter.next();
    assertEquals(uuid.toString(), aid.getId());
    assertEquals("coll2", aid.getCollection());
    assertEquals("auid2", aid.getAuid());
  }

  @Test
  public void testGetArtifactsInAUWithURL() {
    assertFalse(index.getArtifactsInAUWithURL(null, null, null).hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll1", null, null).hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll1", "auid1", null)
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll1", "auid1", "uri")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll1", "auid1", "uri1")
	.hasNext());

    index.indexArtifact(artifact1);
    assertFalse(index.getArtifactsInAUWithURL("coll1", "auid1", "uri")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll1", "auid1", "uri1")
	.hasNext());

    index.commitArtifact("id1");

    Iterator<ArtifactIndexData> iter =
	index.getArtifactsInAUWithURL("coll1", "auid1", "uri");
    assertTrue(iter.hasNext());
    ArtifactIndexData aid = iter.next();
    assertEquals("id1", aid.getId());
    assertEquals("coll1", aid.getCollection());
    assertEquals("auid1", aid.getAuid());
    assertEquals("uri1", aid.getUri());

    iter = index.getArtifactsInAUWithURL("coll1", "auid1", "uri1");
    assertTrue(iter.hasNext());
    aid = iter.next();
    assertEquals("id1", aid.getId());
    assertEquals("coll1", aid.getCollection());
    assertEquals("auid1", aid.getAuid());
    assertEquals("uri1", aid.getUri());

    assertFalse(index.getArtifactsInAUWithURL("coll2", null, null).hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid1", null)
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid1", "uri")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid1", "uri1")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid2", "uri")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid2", "uri1")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid2", "uri2")
	.hasNext());

    index.indexArtifact(artifact2);
    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid1", "uri")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid1", "uri1")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid1", "uri2")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid2", "uri")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid2", "uri1")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid2", "uri2")
	.hasNext());

    index.commitArtifact(uuid.toString());
    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid1", "uri")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid1", "uri1")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid1", "uri2")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid2", "uri1")
	.hasNext());

    iter = index.getArtifactsInAUWithURL("coll2", "auid2", "uri");
    assertTrue(iter.hasNext());
    aid = iter.next();
    assertEquals(uuid.toString(), aid.getId());
    assertEquals("coll2", aid.getCollection());
    assertEquals("auid2", aid.getAuid());
    assertEquals("uri2", aid.getUri());

    iter = index.getArtifactsInAUWithURL("coll2", "auid2", "uri2");
    assertTrue(iter.hasNext());
    aid = iter.next();
    assertEquals(uuid.toString(), aid.getId());
    assertEquals("coll2", aid.getCollection());
    assertEquals("auid2", aid.getAuid());
    assertEquals("uri2", aid.getUri());

    assertFalse(index.getArtifactsInAUWithURL("coll1", "auid1", "uri", "v2")
	.hasNext());

    assertFalse(index.getArtifactsInAUWithURL("coll1", "auid1", "uri1", "v2")
	.hasNext());

    iter = index.getArtifactsInAUWithURL("coll1", "auid1", "uri", "v1");
    assertTrue(iter.hasNext());
    aid = iter.next();
    assertEquals("id1", aid.getId());
    assertEquals("coll1", aid.getCollection());
    assertEquals("auid1", aid.getAuid());
    assertEquals("uri1", aid.getUri());
    assertEquals("v1", aid.getVersion());

    iter = index.getArtifactsInAUWithURL("coll1", "auid1", "uri1", "v1");
    assertTrue(iter.hasNext());
    aid = iter.next();
    assertEquals("id1", aid.getId());
    assertEquals("coll1", aid.getCollection());
    assertEquals("auid1", aid.getAuid());
    assertEquals("uri1", aid.getUri());
    assertEquals("v1", aid.getVersion());

    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid2", "uri", "v1")
	.hasNext());

    assertFalse(index.getArtifactsInAUWithURL("coll2", "auid2", "uri2", "v1")
	.hasNext());

    iter = index.getArtifactsInAUWithURL("coll2", "auid2", "uri", "v2");
    assertTrue(iter.hasNext());
    aid = iter.next();
    assertEquals(uuid.toString(), aid.getId());
    assertEquals("coll2", aid.getCollection());
    assertEquals("auid2", aid.getAuid());
    assertEquals("uri2", aid.getUri());
    assertEquals("v2", aid.getVersion());

    iter = index.getArtifactsInAUWithURL("coll2", "auid2", "uri2", "v2");
    assertTrue(iter.hasNext());
    aid = iter.next();
    assertEquals(uuid.toString(), aid.getId());
    assertEquals("coll2", aid.getCollection());
    assertEquals("auid2", aid.getAuid());
    assertEquals("uri2", aid.getUri());
    assertEquals("v2", aid.getVersion());
  }

  @Test
  public void testGetArtifactsInAUWithURLMatch() {
    assertFalse(index.getArtifactsInAUWithURLMatch(null, null, null)
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll1", null, null)
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll1", "auid1", null)
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll1", "auid1", "uri")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll1", "auid1", "uri1")
	.hasNext());

    index.indexArtifact(artifact1);
    assertFalse(index.getArtifactsInAUWithURLMatch("coll1", "auid1", "uri")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll1", "auid1", "uri1")
	.hasNext());

    index.commitArtifact("id1");
    assertFalse(index.getArtifactsInAUWithURLMatch("coll1", "auid1", "uri")
	.hasNext());

    Iterator<ArtifactIndexData> iter =
	index.getArtifactsInAUWithURLMatch("coll1", "auid1", "uri1");
    assertTrue(iter.hasNext());
    ArtifactIndexData aid = iter.next();
    assertEquals("id1", aid.getId());
    assertEquals("coll1", aid.getCollection());
    assertEquals("auid1", aid.getAuid());
    assertEquals("uri1", aid.getUri());

    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", null, null)
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid1", null)
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid1", "uri")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid1", "uri1")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid2", "uri")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid2", "uri1")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid2", "uri2")
	.hasNext());

    index.indexArtifact(artifact2);
    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid1", "uri")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid1", "uri1")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid1", "uri2")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid2", "uri")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid2", "uri1")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid2", "uri2")
	.hasNext());

    index.commitArtifact(uuid.toString());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid1", "uri")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid1", "uri1")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid1", "uri2")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid2", "uri")
	.hasNext());
    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid2", "uri1")
	.hasNext());

    iter = index.getArtifactsInAUWithURLMatch("coll2", "auid2", "uri2");
    assertTrue(iter.hasNext());
    aid = iter.next();
    assertEquals(uuid.toString(), aid.getId());
    assertEquals("coll2", aid.getCollection());
    assertEquals("auid2", aid.getAuid());
    assertEquals("uri2", aid.getUri());

    assertFalse(index.getArtifactsInAUWithURLMatch("coll1", "auid1", "uri",
	"v2").hasNext());

    assertFalse(index.getArtifactsInAUWithURLMatch("coll1", "auid1", "uri1",
	"v2").hasNext());

    assertFalse(index.getArtifactsInAUWithURLMatch("coll1", "auid1", "uri",
	"v1").hasNext());

    iter = index.getArtifactsInAUWithURLMatch("coll1", "auid1", "uri1", "v1");
    assertTrue(iter.hasNext());
    aid = iter.next();
    assertEquals("id1", aid.getId());
    assertEquals("coll1", aid.getCollection());
    assertEquals("auid1", aid.getAuid());
    assertEquals("uri1", aid.getUri());
    assertEquals("v1", aid.getVersion());

    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid2", "uri",
	"v1").hasNext());

    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid2", "uri2",
	"v1").hasNext());

    assertFalse(index.getArtifactsInAUWithURLMatch("coll2", "auid2", "uri",
	"v2").hasNext());

    iter = index.getArtifactsInAUWithURLMatch("coll2", "auid2", "uri2", "v2");
    assertTrue(iter.hasNext());
    aid = iter.next();
    assertEquals(uuid.toString(), aid.getId());
    assertEquals("coll2", aid.getCollection());
    assertEquals("auid2", aid.getAuid());
    assertEquals("uri2", aid.getUri());
    assertEquals("v2", aid.getVersion());
  }

  @Test
  public void testQuery() {
    ArtifactPredicateBuilder query = new ArtifactPredicateBuilder();
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByCommitStatus(false);
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByCommitStatus(true);
    assertFalse(index.query(query).hasNext());

    index.indexArtifact(artifact1);
    query = new ArtifactPredicateBuilder();
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByCommitStatus(true);
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByCommitStatus(false);
    assertTrue(index.query(query).hasNext());

    query.filterByAuid("auid1");
    assertTrue(index.query(query).hasNext());

    query.filterByCollection("coll1");
    assertTrue(index.query(query).hasNext());

    query.filterByURIPrefix("uri");
    assertTrue(index.query(query).hasNext());

    query.filterByURIPrefix("uri1");
    assertTrue(index.query(query).hasNext());

    query.filterByURIMatch("uri1");
    assertTrue(index.query(query).hasNext());

    query.filterByVersion("v1");
    assertTrue(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByAuid(null);
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByAuid("unknown");
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByCollection(null);
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByCollection("unknown");
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByURIPrefix(null);
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByURIPrefix("unknown");
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByURIMatch(null);
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByURIMatch("unknown");
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByVersion(null);
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByVersion("unknown");
    assertFalse(index.query(query).hasNext());

    index.commitArtifact(artifact1.getIdentifier().getId());
    query = new ArtifactPredicateBuilder();
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByCommitStatus(false);
    assertFalse(index.query(query).hasNext());

    query = new ArtifactPredicateBuilder();
    query.filterByCommitStatus(true);
    assertTrue(index.query(query).hasNext());
  }
}
