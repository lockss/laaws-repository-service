/*
 * Copyright (c) 2017, Board of Trustees of Leland Stanford Jr. University,
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.lockss.laaws.rs.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicStatusLine;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.core.RestLockssRepository;
import org.lockss.laaws.rs.core.VolatileLockssRepository;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.RepositoryArtifactMetadata;
import org.lockss.util.test.LockssTestCase5;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.UUID;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class TestRestLockssRepository extends LockssTestCase5 {
    private final static Log log = LogFactory.getLog(TestRestLockssRepository.class);

    @LocalServerPort
    private int port;

    @Autowired
    ApplicationContext appCtx;

    @TestConfiguration
    static class TestLockssRepositoryConfig {
        @Bean
        public LockssRepository createRepository() {
            return new VolatileLockssRepository();
        }
    }

    private ArtifactIdentifier aid1;
    private ArtifactIdentifier aid2;
    private ArtifactIdentifier aid3;
    private RepositoryArtifactMetadata md1;
    private RepositoryArtifactMetadata md2;
    private RepositoryArtifactMetadata md3;
    private ArtifactData artifactData1;
    private ArtifactData artifactData2;
    private ArtifactData artifactData3;

    private UUID uuid;
    private StatusLine httpStatus;

    protected LockssRepository repository;

    public LockssRepository makeLockssRepository() throws Exception {
      log.info("port = " + port);
        return new RestLockssRepository(new URL(String.format("http://localhost:%d", port)));
    }

    @Before
    public void setUpArtifactDataStore() throws Exception {
        uuid = UUID.randomUUID();

        httpStatus = new BasicStatusLine(
                new ProtocolVersion("HTTP", 1,1),
                200,
                "OK"
        );

        aid1 = new ArtifactIdentifier("id1", "coll1", "auid1", "uri1", 1);
        aid2 = new ArtifactIdentifier(uuid.toString(), "coll2", "auid2", "uri2", 2);
        aid3 = new ArtifactIdentifier("id3", "coll1", "auid1", "uri2", 1);

        md1 = new RepositoryArtifactMetadata(aid1, false, false);
        md2 = new RepositoryArtifactMetadata(aid2, true, false);
        md3 = new RepositoryArtifactMetadata(aid1, false, false);

        artifactData1 = new ArtifactData(aid1, null, new ByteArrayInputStream("bytes1".getBytes()), httpStatus, "surl1", md1);
        artifactData2 = new ArtifactData(aid2, null, new ByteArrayInputStream("bytes2".getBytes()), httpStatus, "surl2", md2);
        artifactData3 = new ArtifactData(aid3, null, new ByteArrayInputStream("bytes3".getBytes()), httpStatus, "surl3", md3);

        this.repository = makeLockssRepository();
    }

    @After
    public void tearDownArtifactDataStore() throws Exception {
        this.repository = null;
    }

    @Test
    public void addArtifact() {
        try {
            // Attempt adding a null artifact and expect IllegalArgumentException to the thrown
            repository.addArtifact(null);
            fail("Attempted to add a null artifact and was expecting IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            String expectedErrMsg = "ArtifactData is null";
            assertEquals(expectedErrMsg, e.getMessage());
        } catch (IOException e) {
            fail("Expected IllegalArgumentException but got IOException");
        }

        try {
            // Add an artifact to the repository
            Artifact artifact = repository.addArtifact(artifactData1);
            assertNotNull(artifact);
            String artifactId = artifact.getId();
            assertNotNull(artifactId);
            assertFalse(repository.isArtifactCommitted(artifact.getCollection(), artifactId));
            assertTrue(repository.artifactExists(artifact.getCollection(), artifactId));

//            ArtifactData artifact = repo.getArtifact("coll1", artifactId);
//            assertNotNull(artifact);
//            assertEquals(artifactData1.getIdentifier().getId(), artifact.getIdentifier().getId());
//
//            assertFalse(repo.getCollectionIds().hasNext());
//            assertFalse(repo.getAuIds("coll1").hasNext());
//
//            repo.commitArtifact("coll1", artifactId);
//            assertFalse(repo.getAuIds("coll2").hasNext());
//            assertTrue(repo.getAuIds("coll1").hasNext());
//            Iterator<String> collectionIds = repo.getCollectionIds();
//            assertEquals("coll1", collectionIds.next());
//            assertFalse(collectionIds.hasNext());
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }
    }

    @Test
    public void getArtifact() throws Exception {
            // Add the artifact and verify we get back an artifact ID
            Artifact artifact = repository.addArtifact(artifactData1);

            assertNotNull(artifact);
            String artifactId = artifact.getId();
            assertNotNull(artifactId);
            assertTrue(repository.artifactExists(artifact.getCollection(), artifactId));

            // Retrieve the artifact and verify we get back the same artifact
            ArtifactData artifactData = repository.getArtifactData("coll1", artifactId);
            assertNotNull(artifactData);
            assertEquals(artifactId, artifactData.getIdentifier().getId());
    }

    @Test
    public void commitArtifact() {
        try {
            // Attempt to commit to a null collection
            repository.commitArtifact(null, null);
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            // OK
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }

        try {
            // Attempt to commit to a null collection
            repository.commitArtifact(null, "doesntMatter");
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            // OK
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }

        try {
            // Attempt to commit to a null artifact id
            repository.commitArtifact("doesntMatter", null);
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            // OK
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }

        try {
            // Add an artifact and verify that it is not committed
            Artifact artifact = repository.addArtifact(artifactData1);
            assertNotNull(artifact);
            String artifactId = artifact.getId();
            assertFalse(repository.isArtifactCommitted(artifact.getCollection(), artifactId));

            // Commit the artifact and verify that it is committed
            repository.commitArtifact(artifactData1.getIdentifier().getCollection(), artifactId);
            assertTrue(repository.isArtifactCommitted(artifact.getCollection(), artifactId));
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }
    }

    @Test
    public void deleteArtifact() {
        final String expectedErrMsg = "Null collection ID or artifact ID";

        try {
            repository.deleteArtifact(null, null);
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            assertEquals(expectedErrMsg, e.getMessage());
        } catch (IOException e) {
            fail("Expected IllegalArgumentException but got IOException");
        }

        try {
            repository.deleteArtifact(artifactData1.getIdentifier().getCollection(), null);
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            assertEquals(expectedErrMsg, e.getMessage());
        } catch (IOException e) {
            fail("Expected IllegalArgumentException but got IOException");
        }

        Artifact artifact = null;

        try {
            // Attempt to add an artifact and verify it exists
            artifact = repository.addArtifact(artifactData1);
            assertNotNull(artifact);
            assertNotNull(artifact.getId());
            assertTrue(repository.artifactExists(artifact.getCollection(), artifact.getId()));
        } catch (IOException e) {
            fail("Expected IllegalArgumentException but got IOException");
        }

        try {
            repository.deleteArtifact(null, artifact.getId());
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            assertEquals(expectedErrMsg, e.getMessage());
        } catch (IOException e) {
            fail("Expected IllegalArgumentException but got IOException");
        }

        try {
            // Delete the artifact and check that it doesn't exist
            repository.deleteArtifact(artifact.getCollection(), artifact.getId());
            assertFalse(repository.artifactExists(artifact.getCollection(), artifact.getId()));
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }
    }

    @Test
    public void artifactExists() {
        String expectedErrMsg = "Null or empty identifier";

        try {
            // Attempt to invoke an IllegalArgumentException
            repository.artifactExists(null, null);
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            assertEquals(expectedErrMsg, e.getMessage());
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }

        try {
            // Attempt to invoke an IllegalArgumentException
            repository.artifactExists("", "");
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            assertEquals(expectedErrMsg, e.getMessage());
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }

        try {
            // Check for something that doesn't exist
            assertFalse(repository.artifactExists("nonExistentCollection", "nonExistentArtifact"));

            // Add an artifact and verify it exists
            Artifact artifact = repository.addArtifact(artifactData1);
            assertNotNull(artifact);
            String artifactId = artifact.getId();
            assertNotNull(artifactId);
            assertTrue(repository.artifactExists(artifact.getCollection(), artifactId));
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }
    }

    @Test
    public void isArtifactCommitted() {
        try {
            repository.isArtifactCommitted(null, null);
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            // OK
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }

        try {
            repository.isArtifactCommitted("","");
            fail("Expected to catch IllegalArgumentException but no exception was thrown");
        } catch (IllegalArgumentException e) {
            // OK
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }

        String artifactId = null;

        try {
            Artifact artifact = repository.addArtifact(artifactData1);
            assertNotNull(artifact);
            artifactId = artifact.getId();
            assertNotNull(artifactId);
            assertTrue(repository.artifactExists(artifact.getCollection(), artifactId));
            assertFalse(repository.isArtifactCommitted(artifact.getCollection(), artifactId));

            repository.commitArtifact(artifactData1.getIdentifier().getCollection(), artifactId);
            assertTrue(repository.isArtifactCommitted(artifact.getCollection(), artifactId));
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }
    }

    @Test
    public void getCollectionIds() {
        try {
            // Nothing added yet
            Iterator<String> collectionIds = repository.getCollectionIds().iterator();
            assertNotNull(collectionIds);
            assertFalse(collectionIds.hasNext());

            // Add an artifact
            Artifact artifact = repository.addArtifact(artifactData1);
            assertNotNull(artifact);
            String artifactId = artifact.getId();
            assertNotNull(artifactId);
            assertTrue(repository.artifactExists(artifact.getCollection(), artifactId));

            // ArtifactData is uncommitted so getCollectionIds() should return nothing
            collectionIds = repository.getCollectionIds().iterator();
            assertNotNull(collectionIds);
            assertFalse(repository.getCollectionIds().iterator().hasNext());

            // Commit artifact and check again
            repository.commitArtifact(artifactData1.getIdentifier().getCollection(), artifactId);
            assertTrue(repository.isArtifactCommitted(artifact.getCollection(), artifactId));
            assertTrue(repository.getCollectionIds().iterator().hasNext());
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }
    }

    @Test
    public void getAuIds() {
        try {
            Iterator<String> auids = repository.getAuIds(null).iterator();
            assertNotNull(auids);
            assertFalse(auids.hasNext());

            Artifact artifact = repository.addArtifact(artifactData1);
            assertNotNull(artifact);
            String artifactId = artifact.getId();
            assertNotNull(artifactId);
            assertTrue(repository.artifactExists(artifact.getCollection(), artifactId));
            assertFalse(repository.isArtifactCommitted(artifact.getCollection(), artifactId));

            auids = repository.getAuIds(artifactData1.getIdentifier().getCollection()).iterator();
            assertNotNull(auids);
            assertFalse(auids.hasNext());

            repository.commitArtifact(artifactData1.getIdentifier().getCollection(), artifactId);

            auids = repository.getAuIds(artifactData1.getIdentifier().getCollection()).iterator();
            assertNotNull(auids);
            assertTrue(auids.hasNext());
        } catch (IOException e) {
            fail(String.format("Unexpected IOException thrown: %s", e));
        }
    }

    @Test
    public void getArtifactsInAU() throws IOException {
        Iterator<Artifact> result = null;

        assertFalse(repository.getAllArtifactsAllVersions(null, null).iterator().hasNext());
        assertFalse(repository.getAllArtifactsAllVersions(null, "unknown").iterator().hasNext());
        assertFalse(repository.getAllArtifactsAllVersions("unknown", null).iterator().hasNext());
        assertFalse(repository.getAllArtifactsAllVersions("unknown", "unknown").iterator().hasNext());

        Artifact addedArtifact = repository.addArtifact(artifactData1);
        assertNotNull(addedArtifact);
        assertNotNull(repository.addArtifact(artifactData2));

        assertFalse(repository.getAllArtifactsAllVersions(aid1.getCollection(), aid1.getAuid()).iterator().hasNext());

        repository.commitArtifact(aid1.getCollection(), addedArtifact.getId());

        result = repository.getAllArtifactsAllVersions(aid1.getCollection(), aid1.getAuid()).iterator();
        assertNotNull(result);
        assertTrue(result.hasNext());

        Artifact indexData = result.next();
        assertNotNull(indexData);
        assertFalse(result.hasNext());
        assertNotEquals(aid1.getId(), indexData.getIdentifier().getId());
    }

    @Test
    public void getArtifactsInAUWithURL() throws IOException {
        assertNotNull(repository.addArtifact(artifactData1));
        assertNotNull(repository.addArtifact(artifactData2));
        assertNotNull(repository.addArtifact(artifactData3));

        Iterator<Artifact> result = null;

//            repo.commitArtifact(aid1.getCollection(), aid1.getId());

        assertFalse(repository.getAllArtifactsWithPrefixAllVersions(null, null, null).iterator().hasNext());
        assertFalse(repository.getAllArtifactsWithPrefixAllVersions(aid1.getCollection(), null, null).iterator().hasNext());
        assertFalse(repository.getAllArtifactsWithPrefixAllVersions(null, aid1.getAuid(), null).iterator().hasNext());
        assertFalse(repository.getAllArtifactsWithPrefixAllVersions(null, null, "url").iterator().hasNext());
        assertFalse(repository.getAllArtifactsWithPrefixAllVersions(aid1.getCollection(), aid1.getAuid(), null).iterator().hasNext());
        assertFalse(repository.getAllArtifactsWithPrefixAllVersions(aid1.getCollection(), null,  "url").iterator().hasNext());
        assertFalse(repository.getAllArtifactsWithPrefixAllVersions(null, aid1.getAuid(),  "url").iterator().hasNext());
        assertFalse(repository.getAllArtifactsWithPrefixAllVersions(aid1.getCollection(), aid1.getAuid(),  "url").iterator().hasNext());

        Artifact addedArtifact = repository.addArtifact(artifactData1);
        assertNotNull(addedArtifact);
        assertNotNull(repository.addArtifact(artifactData2));
        assertNotNull(repository.addArtifact(artifactData3));

        assertFalse(repository.getAllArtifactsWithPrefixAllVersions(aid1.getCollection(), aid1.getAuid(), aid1.getUri()).iterator().hasNext());

        repository.commitArtifact(aid1.getCollection(), addedArtifact.getId());

        result = repository.getAllArtifactsWithPrefixAllVersions(aid1.getCollection(), aid1.getAuid(), aid1.getUri()).iterator();
        assertNotNull(result);
        assertTrue(result.hasNext());

        Artifact indexData = result.next();
        assertNotNull(indexData);
        assertFalse(result.hasNext());
        assertNotEquals(aid1.getId(), indexData.getIdentifier().getId());
        assertEquals(aid1.getUri(), indexData.getIdentifier().getUri());
    }

    @Test
    public void getArtifactsInAUWithURLMatch() {
    }

    @Test
    public void testGetArtifactAllVersions() {

    }

    @Test
    public void testGetArtifact() throws IOException {
        // Add two versions of URL
        Artifact artifact = repository.addArtifact(artifactData1);
        assertNotNull(artifact);
        repository.commitArtifact(artifact);
        assertEquals((long)1, (long)artifact.getVersion());

        artifact = repository.addArtifact(artifactData1);
        assertNotNull(artifact);
        repository.commitArtifact(artifact);
        assertEquals((long)2, (long)artifact.getVersion());

        // Add a third version of URL but don't commit its artifact
        artifact = repository.addArtifact(artifactData1);
        assertNotNull(artifact);
        assertEquals((long)3, (long)artifact.getVersion());

        // Get latest committed artifact
        artifact = repository.getArtifact("coll1", "auid1", "uri1");
        assertNotNull(artifact);
        assertEquals((long)2, (long)artifact.getVersion());

        // Add fourth version and commit
        artifact = repository.addArtifact(artifactData1);
        assertNotNull(artifact);
        repository.commitArtifact(artifact);
        assertEquals((long)4, (long)artifact.getVersion());

        // Get latest artifact
        artifact = repository.getArtifact("coll1", "auid1", "uri1");
        assertNotNull(artifact);
        assertEquals((long)4, (long)artifact.getVersion());
    }

    @Test
    public void testGetArtifactVersion() throws IOException {
        // Add three versions of a URL
        Artifact artifact = repository.addArtifact(artifactData1);
        assertNotNull(artifact);
        repository.commitArtifact(artifact);
        assertEquals((long)1, (long)artifact.getVersion());

        artifact = repository.addArtifact(artifactData1);
        assertNotNull(artifact);
        repository.commitArtifact(artifact);
        assertEquals((long)2, (long)artifact.getVersion());

        artifact = repository.addArtifact(artifactData1);
        assertNotNull(artifact);
        repository.commitArtifact(artifact);
        assertEquals((long)3, (long)artifact.getVersion());

        // Non-existent version
        artifact = repository.getArtifactVersion("coll1", "auid1", "uri1", 4);
        assertNull(artifact);

        // Retrieve second version
        artifact = repository.getArtifactVersion("coll1", "auid1", "uri1", 2);
        assertNotNull(artifact);
        assertEquals((long)2, (long)artifact.getVersion());
    }

    @Test
    public void testAuSize() throws IOException {
        // Test result of non-existent collection and AU
        long size = repository.auSize("sunset", "sunrise");
        assertNotNull(size);
        assertEquals(0, size);

        // Add some artifacts
        Artifact artifact = repository.addArtifact(artifactData1);
        repository.commitArtifact(artifact);
        artifact = repository.addArtifact(artifactData2);
        repository.commitArtifact(artifact);
        artifact = repository.addArtifact(artifactData3);

        // Check size of auid1
        size = repository.auSize("coll1", "auid1");
        assertNotNull(size);
        assertEquals(6, size);

        // Commit second artifact to auid1
        repository.commitArtifact(artifact);

        // Check size of auid1
        size = repository.auSize("coll1", "auid1");
        assertNotNull(size);
        assertEquals(12, size);

        // Check size of auid2
        size = repository.auSize("coll2", "auid2");
        assertNotNull(size);
        assertEquals(6, size);

        // Delete an artifact from auid1 and check size
        repository.deleteArtifact(artifact);

        // Check size of auid1
        size = repository.auSize("coll1", "auid1");
        assertNotNull(size);
        assertEquals(6, size);
    }
}
