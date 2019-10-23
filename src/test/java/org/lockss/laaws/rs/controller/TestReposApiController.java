/*
 * Copyright (c) 2017-2019, Board of Trustees of Leland Stanford Jr. University,
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.lockss.laaws.rs.api.CollectionsApiController;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.model.AuidPageInfo;
import org.lockss.util.test.LockssTestCase5;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.multipart.MultipartFile;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(SpringRunner.class)
@WebMvcTest(CollectionsApiController.class)
@AutoConfigureMockMvc(secure = false)
@ComponentScan(basePackages = { "org.lockss.laaws.rs",
    "org.lockss.laaws.rs.api" })
public class TestReposApiController extends LockssTestCase5 {
    private final static Log log = LogFactory.getLog(TestReposApiController.class);

    @Autowired
    private MockMvc controller;

//    @MockBean
//    private ArtifactIndex artifactIndex;

//    @MockBean
//    private ArtifactDataStore artifactStore;

    @MockBean
    private LockssRepository repo;



//    @TestConfiguration
//    public static class RepoControllerTestConfig {
//        @Bean
//        public LockssArtifactClientRepository setRepository() {
//            return new MockLockssArtifactRepositoryImpl();
//        }
//    }
//
//    @Before
//    public void setUp() throws Exception {
//        // Populate the repository with test stuff
//    }
//
//    @After
//    public void tearDown() throws Exception {
//        // Nothing? Let it the JVM perform a GC
//    }

    @Test
    public void getCollections() throws Exception {
        // Perform tests against a repository service that is not ready (should expect 503)
        given(this.repo.isReady()).willReturn(false);
        assertFalse(repo.isReady());
        this.controller.perform(get("/collections")).andExpect(status().isServiceUnavailable());

        // Perform tests against a ready repository service
        given(this.repo.isReady()).willReturn(true);

        // Set of collections IDs; start empty
        List<String> collectionIds = new ArrayList<>();

        // Assert that we get an empty set of collection IDs from the controller if repository returns empty set
        given(this.repo.getCollectionIds()).willReturn(collectionIds);
        this.controller.perform(get("/collections")).andExpect(status().isOk()).andExpect(
            content().string("[]"));

        // Add collection IDs our set
        collectionIds.add("test1");
        collectionIds.add("test2");

        // Assert that we get back the same set of collection IDs
        given(this.repo.getCollectionIds()).willReturn(collectionIds);
        this.controller.perform(get("/collections")).andExpect(status().isOk()).andExpect(
            content().string("[\"test1\",\"test2\"]"));
    }

    /**
     * Tests the endpoint used to get the auids for a given collection.
     * 
     * @throws Exception if there are problems.
     */
    @Test
    public void getAuids() throws Exception {
      // The mapper of received content text to a page information object.
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
	  false);

      String collId = "collId";
      String endpointUrl = "/collections/" + collId + "/aus";

      // Perform tests against a repository service that is not ready (should
      // expect 503).
      given(repo.isReady()).willReturn(false);
      assertFalse(repo.isReady());

      controller.perform(get(endpointUrl))
      .andExpect(status().isServiceUnavailable());

      // Perform tests against a ready repository service.
      given(repo.isReady()).willReturn(true);

      // Set up the collection.
      List<String> collectionIds = new ArrayList<>();
      collectionIds.add(collId);
      given(repo.getCollectionIds()).willReturn(collectionIds);

      // Set of auids - Start empty.
      List<String> auids = new ArrayList<>();

      // The repository will return an empty set.
      given(repo.getAuIds(collId)).willReturn(auids);

      // Perform the request and get the response.
      String content =  controller.perform(get(endpointUrl))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      AuidPageInfo api = mapper.readValue(content, AuidPageInfo.class);

      // Assert that we get an empty set of auids from the controller.
      assertEquals(0, api.getAuids().size());

      // Add auids to the collection that the repository will return.
      auids.add("test1");
      auids.add("test2");

      // Perform the request and get the response.
      content = controller.perform(get(endpointUrl)).andExpect(status().isOk())
	  .andReturn().getResponse().getContentAsString();

      // Get the auids included in the response.
      List<String> auidBuffer =
	  mapper.readValue(content, AuidPageInfo.class).getAuids();
      
      // Assert that we get back the same set of auids.
      assertEquals(2, auidBuffer.size());
      assertEquals("test1", auidBuffer.get(0));
      assertEquals("test2", auidBuffer.get(1));

      // Request the first page containing just one auid.
      content = controller.perform(get(endpointUrl + "?limit=1"))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      api = mapper.readValue(content, AuidPageInfo.class);

      // Get the first auid included in the response.
      auidBuffer = api.getAuids();
      assertEquals(1, auidBuffer.size());
      assertEquals("test1", auidBuffer.get(0));

      // There are more auids to be returned.
      assertTrue(api.getPageInfo().getContinuationToken() != null);

      // Get the link needed to get the next page.
      String nextLink = api.getPageInfo().getNextLink();
      assertTrue(nextLink != null);

      // Request the next page.
      content = controller.perform(get(new URL(nextLink).getFile()))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      api = mapper.readValue(content, AuidPageInfo.class);

      // Get the second auid included in the response.
      auidBuffer = api.getAuids();
      assertEquals(1, auidBuffer.size());
      assertEquals("test2", auidBuffer.get(0));

      // There are no more auids to be returned.
      assertTrue(api.getPageInfo().getContinuationToken() == null);
      assertTrue(api.getPageInfo().getNextLink() == null);
    }

    @Test
    public void reposRepositoryArtifactsArtifactidDelete() throws Exception {
    }

    @Test
    public void reposRepositoryArtifactsArtifactidGet() throws Exception {
    }

    @Test
    public void reposRepositoryArtifactsArtifactidPut() throws Exception {
    }

    @Test
    public void reposRepositoryArtifactsGet() throws Exception {
    }


    /**
     * Test POST-ing with WARC record
     */
    @Test
    public void testPostArtifactWithWARCRecord() throws Exception {

    }

    /**
     * Test POST-ing with HTTP response stream
     */
    @Test
    public void testPostArtifactWithHTTPResponse() throws Exception {
//        MultipartFile payload = new MockMultipartFile("content", "http-response", "application/http; msgtype=response", SAMPLE_HTTP_RESPONSE_BYTES);

        /*
        ResponseEntity<RepositoryArtifact> response = controller.reposRepositoryArtifactsPost( "test", "test", "test", null, null, payload, null);

        RepositoryArtifact artifact = response.getBody();

        assertNotNull(artifact.getId());
        assertThat(artifact.getRepository(), is("test"));
        assertThat(artifact.getAuid(), is("test"));

        InputStream content = artifact.getInputStream();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(content, baos);

        log.info(baos.toString());
        log.info(baos.size());
        */
    }


    /**
     * Test POST-ing raw content and headers
     * @throws Exception
     */
    @Test
    public void testPostArtifactWithContent() throws Exception {
        /*
        // Create some mock MultipartFile objects
        MultipartFile content = new MockMultipartFile("content", "jake.txt", "text/html", JAKE);
        MultipartFile metadata = new MockMultipartFile("metadata","bmo.txt","text/html", BMO);

        // Attempt to create a new artifact and make sure we get a ResponseEntity
        Object response = controller.reposRepositoryArtifactsPost("test", "xyzzy", "quote", null, null, content, metadata);
        assertThat(response, instanceOf(ResponseEntity.class));

        // Check that an ArtifactData is encapsulated in the body
        Object body = ((ResponseEntity<Object>)response).getBody();
        assertThat(body, instanceOf(AbstractArtifact.class));

        AbstractArtifact artifact = (AbstractArtifact)body;
        assertNotNull(artifact.getId());
        assertEquals(artifact.getAuid(), "xyzzy");
        assertEquals(artifact.getUri(), "quote");

        //InputStream is = artifact.getContentStream();
        InputStream is = artifact.getInputStream();

        //String b = IOUtils.toString(is);
        byte[] b = IOUtils.toByteArray(is);
        log.error("OKKKKKKKKK " + new String(b));
        log.error(JAKE.length + " " + b.length);
        assertTrue(Arrays.equals(JAKE, b));
        */

    }
}