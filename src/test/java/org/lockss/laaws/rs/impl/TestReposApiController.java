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

package org.lockss.laaws.rs.impl;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.lockss.laaws.rs.api.RepoinfoApiController;
import org.lockss.laaws.rs.controller.DefaultTestRepositoryApplicationConfiguration;
import org.lockss.log.L4JLogger;
import org.lockss.rs.BaseLockssRepository;
import org.lockss.spring.test.SpringLockssTestCase4;
import org.lockss.util.UrlUtil;
import org.lockss.util.io.FileUtil;
import org.lockss.util.rest.repo.model.Artifact;
import org.lockss.util.rest.repo.model.ArtifactPageInfo;
import org.lockss.util.rest.repo.model.AuidPageInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@WebMvcTest(RepoinfoApiController.class)
@AutoConfigureMockMvc()
// We can't use a @ContextConfiguration here because the tests rely on a @MockBean to mock the
// internal BaseLockssRepository behavior.
public class TestReposApiController extends SpringLockssTestCase4 {
    private final static L4JLogger log = L4JLogger.getLogger();

    @Autowired
    private MockMvc controller;

    @MockBean
    private BaseLockssRepository repo;

    // The value of the Authorization header to be used when calling the REST
    // service.
    private String authHeaderValue = null;

    @Test
    public void getNamespaces() throws Exception {
        log.debug2("Invoked");

        // Perform tests against a repository service that is not ready (should expect 503)
        given(this.repo.isReady()).willReturn(false);
        assertFalse(repo.isReady());

        this.controller.perform(getAuthBuilder(get("/namespaces")))
        .andExpect(status().isServiceUnavailable());

        // Perform tests against a ready repository service
        given(this.repo.isReady()).willReturn(true);

        // Set of namespaces; start empty
        List<String> namespaces = new ArrayList<>();

        // Assert that we get an empty set of namespaces from the controller if repository returns empty set
        given(this.repo.getNamespaces()).willReturn(namespaces);
        this.controller.perform(getAuthBuilder(get("/namespaces")))
        .andExpect(status().isOk()).andExpect(content().string("[]"));

        // Add namespaces to our set
        namespaces.add("test1");
        namespaces.add("test2");

        // Assert that we get back the same set of namespaces
        given(this.repo.getNamespaces()).willReturn(namespaces);
        this.controller.perform(getAuthBuilder(get("/namespaces")))
        .andExpect(status().isOk())
        .andExpect(content().string("[\"test1\",\"test2\"]"));
        log.debug2("Done");
    }

    /**
     * Tests the endpoint used to get the auids for a given namespace.
     *
     * @throws Exception if there are problems.
     */
    @Test
    public void getAuids() throws Exception {
      log.debug2("Invoked");
      // The mapper of received content text to a page information object.
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
	  false);

      String namespace = "ns/Id:ABC";
      URI endpointUri = UriComponentsBuilder.fromUriString("/aus")
          .queryParam("namespace", namespace)
          .build().toUri();

      // Perform tests against a repository service that is not ready (should
      // expect 503).
      given(repo.isReady()).willReturn(false);
      assertFalse(repo.isReady());

      controller.perform(getAuthBuilder(get(endpointUri)))
      .andExpect(status().isServiceUnavailable());

      // Perform tests against a ready repository service.
      given(repo.isReady()).willReturn(true);

      // Set up the namespaces.
      List<String> namespaces = new ArrayList<>();
      namespaces.add(namespace);
      given(repo.getNamespaces()).willReturn(namespaces);

      // Set of auids - Start empty.
      List<String> auids = new ArrayList<>();

      // The repository will return an empty set.
      given(repo.getAuIds(namespace)).willReturn(auids);

      // Perform the request and get the response.
      String content =  controller.perform(getAuthBuilder(get(endpointUri)))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      AuidPageInfo api = mapper.readValue(content, AuidPageInfo.class);

      // Assert that we get an empty set of auids from the controller.
      assertEquals(0, api.getAuids().size());

      // Add auids to the namespace that the repository will return.
      auids.add("test01");
      auids.add("test02");

      // Perform the request and get the response.
      content = controller.perform(getAuthBuilder(get(endpointUri)))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Get the auids included in the response.
      List<String> auidBuffer =
	  mapper.readValue(content, AuidPageInfo.class).getAuids();

      // Assert that we get back the same set of auids.
      assertEquals(2, auidBuffer.size());
      assertEquals("test01", auidBuffer.get(0));
      assertEquals("test02", auidBuffer.get(1));

      // There are no more auids to be returned.
      assertNull(api.getPageInfo().getContinuationToken());
      assertNull(api.getPageInfo().getNextLink());

      // Add more auids to the namespace that the repository will return.
      auids.add("test03");
      auids.add("test04");
      auids.add("test05");
      auids.add("test06");
      auids.add("test07");
      auids.add("test08");
      auids.add("test09");
      auids.add("test10");

      // Request the first page containing just three auid.
      endpointUri = UriComponentsBuilder.fromUriString("/aus")
          .queryParam("namespace", namespace)
          .queryParam("limit", 3)
          .build().toUri();

      content = controller.perform(getAuthBuilder(get(endpointUri)))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      api = mapper.readValue(content, AuidPageInfo.class);

      // Get the first three auids included in the response.
      auidBuffer = api.getAuids();
      assertEquals(3, auidBuffer.size());
      assertEquals("test01", auidBuffer.get(0));
      assertEquals("test02", auidBuffer.get(1));
      assertEquals("test03", auidBuffer.get(2));

      // There are more auids to be returned.
      String continuationToken = api.getPageInfo().getContinuationToken();
      assertNotNull(continuationToken);

      // Get the iterator hash code.
      Integer iteratorHashCode =
	  new AuidContinuationToken(continuationToken).getIteratorHashCode();
      assertNotNull(iteratorHashCode);

      // Get the link needed to get the next page.
      String nextLink = api.getPageInfo().getNextLink();
      assertNotNull(nextLink);

      // Request the next page.
      content = controller.perform(getAuthBuilder(get(new URI(nextLink))))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      api = mapper.readValue(content, AuidPageInfo.class);

      // Get the second group of three auids included in the response.
      auidBuffer = api.getAuids();
      assertEquals(3, auidBuffer.size());
      assertEquals("test04", auidBuffer.get(0));
      assertEquals("test05", auidBuffer.get(1));
      assertEquals("test06", auidBuffer.get(2));

      // There are more auids to be returned.
      continuationToken = api.getPageInfo().getContinuationToken();
      assertNotNull(continuationToken);

      // Verify that the iterator hash code is the same.
      assertEquals(iteratorHashCode,
	  new AuidContinuationToken(continuationToken).getIteratorHashCode());

      // Get the link needed to get the next page.
      nextLink = api.getPageInfo().getNextLink();
      assertNotNull(nextLink);

      // Remove the last digit of the continuation token, resulting in the
      // specification of a different iterator hash code.
      URI nextLinkUri = UriComponentsBuilder.fromHttpUrl(nextLink)
          .replaceQueryParam("continuationToken",
              continuationToken.substring(0, continuationToken.length() - 1))
          .build(true).toUri();

      // Request the next page.
      content = controller.perform(getAuthBuilder(get(nextLinkUri)))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      api = mapper.readValue(content, AuidPageInfo.class);

      // Get the third group of three auids included in the response.
      auidBuffer = api.getAuids();
      assertEquals(3, auidBuffer.size());
      assertEquals("test07", auidBuffer.get(0));
      assertEquals("test08", auidBuffer.get(1));
      assertEquals("test09", auidBuffer.get(2));

      // There are more auids to be returned.
      continuationToken = api.getPageInfo().getContinuationToken();
      assertNotNull(continuationToken);

      // Verify that the iterator hash code is not the same.
      assertNotEquals(iteratorHashCode,
	  new AuidContinuationToken(continuationToken).getIteratorHashCode());

      // Get the link needed to get the next page.
      nextLink = api.getPageInfo().getNextLink();
      assertNotNull(nextLink);

      // Request the next page.
      content = controller.perform(getAuthBuilder(get(new URI(nextLink))))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      api = mapper.readValue(content, AuidPageInfo.class);

      // Get the last auid included in the response.
      auidBuffer = api.getAuids();
      assertEquals(1, auidBuffer.size());
      assertEquals("test10", auidBuffer.get(0));

      // There are no more auids to be returned.
      assertNull(api.getPageInfo().getContinuationToken());
      assertNull(api.getPageInfo().getNextLink());
      log.debug2("Done");
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

    /**
     * Tests the endpoint used to get the artifacts for a given AU in a given
     * namespace.
     *
     * @throws Exception if there are problems.
     */
    @Test
    public void reposRepositoryArtifactsGet() throws Exception {
      log.debug2("Invoked");

      // The mapper of received content text to a page information object.
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
	  false);

      String namespace = "ns/Id:ABC";
      String auId = "org|lockss|plugin|TestPlugin&"
	  + "base_url~http://test.com/&journal_issn~1234-5678&volume_name~987";
//      URI endpointUri =
//          UriComponentsBuilder.fromUriString("/aus/" + UrlUtil.encodeUrl(auId) + "/artifacts")
//              .queryParam("namespace", namespace)
//              .queryParam("version", "all")
//              .queryParam("limit", 9)
//              .build().toUri();

      URI endpointUri =
          new URI("/aus/" + UrlUtil.encodeUrl(auId) + "/artifacts?namespace="+UrlUtil.encodeUrl(namespace)+
              "&version" +
              "=all&limit=9");

      // Perform tests against a repository service that is not ready (should
      // expect 503).
      given(repo.isReady()).willReturn(false);
      assertFalse(repo.isReady());

      controller.perform(getAuthBuilder(get(endpointUri))).andExpect(status()
	  .isServiceUnavailable());

      // Perform tests against a ready repository service.
      given(repo.isReady()).willReturn(true);

      // Set up the namespace.
      List<String> namespaces = new ArrayList<>();
      namespaces.add(namespace);
      given(repo.getNamespaces()).willReturn(namespaces);

      // Set up the AU.
      List<String> auIds = new ArrayList<>();
      auIds.add(auId);
      given(repo.getAuIds(namespace)).willReturn(auIds);

      // Set of artifacts - Start empty.
      List<Artifact> artifacts = new ArrayList<>();

      // The repository will return an empty set.
      given(repo.getArtifactsAllVersions(namespace, auId)).willReturn(artifacts);

      // Perform the request and get the response.
      String content =  controller.perform(getAuthBuilder(get(endpointUri)))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      ArtifactPageInfo api = mapper.readValue(content, ArtifactPageInfo.class);

      // Assert that we get an empty set of artifacts from the controller.
      assertEquals(0, api.getArtifacts().size());

      // Add artifacts to the namespace that the repository will return.
      Artifact art1 = new Artifact("test01", namespace, auId, "http://u1", 1, true,
	  "surl", 1, null);
      artifacts.add(art1);
      Artifact art2 = new Artifact("test02", namespace, auId, "http://u2", 1, true,
	  "surl", 1, null);
      artifacts.add(art2);
      Artifact art3 = new Artifact("test03", namespace, auId, "http://u2/b", 1,
	  true, "surl", 1, null);
      artifacts.add(art3);
      Artifact art4 = new Artifact("test04", namespace, auId, "http://u2,a", 1,
	  true, "surl", 1, null);
      artifacts.add(art4);
      Artifact art5 = new Artifact("test05", namespace, auId, "http://u3", 3, true,
	  "surl", 1, null);
      artifacts.add(art5);
      Artifact art6 = new Artifact("test06", namespace, auId, "http://u3", 2, true,
	  "surl", 1, null);
      artifacts.add(art6);
      Artifact art7 = new Artifact("test07", namespace, auId, "http://u4", 8, true,
	  "surl", 1, null);
      artifacts.add(art7);
      Artifact art8 = new Artifact("test08", namespace, auId, "http://u4", 4, true,
	  "surl", 1, null);
      artifacts.add(art8);
      Artifact art9 = new Artifact("test09", namespace, auId, "http://u4", 1, true,
	  "surl", 1, null);
      artifacts.add(art9);

      // Perform the request and get the response.
      content = controller.perform(getAuthBuilder(get(endpointUri)))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Get the artifacts included in the response.
      List<Artifact> artifactBuffer =
	  mapper.readValue(content, ArtifactPageInfo.class).getArtifacts();

      // Assert that we get back the same set of artifacts.
      assertEquals(9, artifactBuffer.size());
      assertEquals(art1, artifactBuffer.get(0));
      assertEquals(art2, artifactBuffer.get(1));
      assertEquals(art3, artifactBuffer.get(2));
      assertEquals(art4, artifactBuffer.get(3));
      assertEquals(art5, artifactBuffer.get(4));
      assertEquals(art6, artifactBuffer.get(5));
      assertEquals(art7, artifactBuffer.get(6));
      assertEquals(art8, artifactBuffer.get(7));
      assertEquals(art9, artifactBuffer.get(8));

      // There are no more artifacts to be returned.
      assertNull(api.getPageInfo().getContinuationToken());
      assertNull(api.getPageInfo().getNextLink());

      // Test the pagination for all versions.
      runAllVersionsPaginationTest(namespace, auId, artifacts, mapper);

      // Test the pagination for all versions with a prefix.
      runUrlPrefixPaginationTest(namespace, auId, "http://u", artifacts, mapper);
      log.debug2("Done");
    }

    private void runAllVersionsPaginationTest(String namespace, String auId,
	List<Artifact> artifacts, ObjectMapper mapper) throws Exception {
      log.debug2("Invoked");
      // Request the first page containing just two artifacts.
//      URI endpointUri =
//          UriComponentsBuilder.fromUriString("/aus/" + UrlUtil.encodeUrl(auId) + "/artifacts")
//              .queryParam("namespace", namespace)
//              .queryParam("version", "all")
//              .queryParam("limit", 2)
//              .build().toUri();

      URI endpointUri = new URI("/aus/" + UrlUtil.encodeUrl(auId) + "/artifacts?namespace="+UrlUtil.encodeUrl(namespace)+
          "&version=all&limit=2");

      String content = controller.perform(getAuthBuilder(get(endpointUri)))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      ArtifactPageInfo api = mapper.readValue(content, ArtifactPageInfo.class);

      // Get the first two artifacts included in the response.
      List<Artifact> artifactBuffer = api.getArtifacts();
      assertEquals(2, artifactBuffer.size());
      assertEquals(artifacts.get(0), artifactBuffer.get(0));
      assertEquals(artifacts.get(1), artifactBuffer.get(1));

      // There are more artifacts to be returned.
      String continuationToken = api.getPageInfo().getContinuationToken();
      assertNotNull(continuationToken);

      // Get the iterator hash code.
      Integer iteratorHashCode = new ArtifactContinuationToken(
	  continuationToken).getIteratorHashCode();
      assertNotNull(iteratorHashCode);

      // Get the link needed to get the next page.
      String nextLink = api.getPageInfo().getNextLink();
      assertNotNull(nextLink);

      // Request the next page.
      content = controller.perform(getAuthBuilder(get(new URI(nextLink))))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      api = mapper.readValue(content, ArtifactPageInfo.class);

      // Get the second group of two artifacts included in the response.
      artifactBuffer = api.getArtifacts();
      assertEquals(2, artifactBuffer.size());
      assertEquals(artifacts.get(2), artifactBuffer.get(0));
      assertEquals(artifacts.get(3), artifactBuffer.get(1));

      // There are more artifacts to be returned.
      continuationToken = api.getPageInfo().getContinuationToken();
      assertNotNull(continuationToken);

      // Verify that the new iterator hash code is the same.
      assertEquals(iteratorHashCode, new ArtifactContinuationToken(
	  continuationToken).getIteratorHashCode());

      // Get the link needed to get the next page.
      nextLink = api.getPageInfo().getNextLink();
      assertNotNull(nextLink);

      // Remove the last digit of the continuation token, resulting in the
      // specification of a different iterator hash code.
      URI nextLinkUri = UriComponentsBuilder.fromHttpUrl(nextLink)
          .replaceQueryParam("continuationToken",
              UrlUtil.encodeUrl(continuationToken.substring(0, continuationToken.length() - 1)))
          .build(true).toUri();

      // Request the next page.
      content = controller.perform(getAuthBuilder(get(nextLinkUri)))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      api = mapper.readValue(content, ArtifactPageInfo.class);

      // Get the third group of two artifacts included in the response.
      artifactBuffer = api.getArtifacts();
      assertEquals(2, artifactBuffer.size());
      assertEquals(artifacts.get(4), artifactBuffer.get(0));
      assertEquals(artifacts.get(5), artifactBuffer.get(1));

      // There are more artifacts to be returned.
      continuationToken = api.getPageInfo().getContinuationToken();
      assertNotNull(continuationToken);

      // Get the new iterator hash code.
      Integer newIteratorHashCode = new ArtifactContinuationToken(
	  continuationToken).getIteratorHashCode();
      assertNotNull(newIteratorHashCode);

      // Verify that the new iterator hash code is not the same.
      assertNotEquals(iteratorHashCode, newIteratorHashCode);

      // Get the link needed to get the next page.
      nextLink = api.getPageInfo().getNextLink();
      assertNotNull(nextLink);

      // Request the next page.
      content = controller.perform(getAuthBuilder(get(new URI(nextLink))))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      api = mapper.readValue(content, ArtifactPageInfo.class);

      // Get the fourth group of two artifacts included in the response.
      artifactBuffer = api.getArtifacts();
      assertEquals(2, artifactBuffer.size());
      assertEquals(artifacts.get(6), artifactBuffer.get(0));
      assertEquals(artifacts.get(7), artifactBuffer.get(1));

      // There are more artifacts to be returned.
      continuationToken = api.getPageInfo().getContinuationToken();
      assertNotNull(continuationToken);

      // Verify that the new iterator hash code is the same.
      assertEquals(newIteratorHashCode, new ArtifactContinuationToken(
	  continuationToken).getIteratorHashCode());

      // Get the link needed to get the next page.
      nextLink = api.getPageInfo().getNextLink();
      assertNotNull(nextLink);

      // Request the next page.
      content = controller.perform(getAuthBuilder(get(new URI(nextLink))))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      api = mapper.readValue(content, ArtifactPageInfo.class);

      // Get the last artifact included in the response.
      artifactBuffer = api.getArtifacts();
      assertEquals(1, artifactBuffer.size());
      assertEquals(artifacts.get(8), artifactBuffer.get(0));

      // There are no more artifacts to be returned.
      assertNull(api.getPageInfo().getContinuationToken());
      assertNull(api.getPageInfo().getNextLink());
      log.debug2("Done");
    }

    private void runUrlPrefixPaginationTest(String namespace, String auId,
	String urlPrefix, List<Artifact> artifacts, ObjectMapper mapper)
	    throws Exception {
      log.debug2("Invoked");
      // The repository will return the set of artifacts with the URL prefix.
      given(repo.getArtifactsWithPrefixAllVersions(namespace, auId, urlPrefix))
      .willReturn(artifacts);

//      Map<String, Object> uriVars = new HashMap<>();
//      uriVars.put("auId", UrlUtil.encodeUrl(auId));
//
//      // Request the first page containing just two artifacts by prefix.
//      URI newEndpoint =
//          UriComponentsBuilder.fromPath("/aus/{auId}/artifacts")
//              .queryParam("namespace", UrlUtil.encodeQueryArg(namespace))
//              .queryParam("version", "all")
//              .queryParam("limit", 2)
//              .queryParam("urlPrefix", urlPrefix)
//              .buildAndExpand(uriVars).toUri();

      URI endpointUri =
          new URI("/aus/" + UrlUtil.encodeUrl(auId) + "/artifacts?namespace="+UrlUtil.encodeUrl(namespace)+
              "&version" +
              "=all&limit=2"
              + "&urlPrefix=" + UrlUtil.encodeUrl(urlPrefix));

      String content = controller.perform(getAuthBuilder(get(endpointUri)))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      ArtifactPageInfo api = mapper.readValue(content, ArtifactPageInfo.class);

      // Get the first two artifacts included in the response.
      List<Artifact> artifactBuffer = api.getArtifacts();
      assertEquals(2, artifactBuffer.size());
      assertEquals(artifacts.get(0), artifactBuffer.get(0));
      assertEquals(artifacts.get(1), artifactBuffer.get(1));

      // There are more artifacts to be returned.
      String continuationToken = api.getPageInfo().getContinuationToken();
      assertNotNull(continuationToken);

      // Get the iterator hash code.
      Integer iteratorHashCode = new ArtifactContinuationToken(
	  continuationToken).getIteratorHashCode();
      assertNotNull(iteratorHashCode);

      // Get the link needed to get the next page.
      String nextLink = api.getPageInfo().getNextLink();
      assertNotNull(nextLink);

      // Request the next page.
      content = controller.perform(getAuthBuilder(get(new URI(nextLink))))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      api = mapper.readValue(content, ArtifactPageInfo.class);

      // Get the second group of two artifacts included in the response.
      artifactBuffer = api.getArtifacts();
      assertEquals(2, artifactBuffer.size());
      assertEquals(artifacts.get(2), artifactBuffer.get(0));
      assertEquals(artifacts.get(3), artifactBuffer.get(1));

      // There are more artifacts to be returned.
      continuationToken = api.getPageInfo().getContinuationToken();
      assertNotNull(continuationToken);

      // Verify that the new iterator hash code is the same.
      assertEquals(iteratorHashCode, new ArtifactContinuationToken(
	  continuationToken).getIteratorHashCode());

      // Get the link needed to get the next page.
      nextLink = api.getPageInfo().getNextLink();
      assertNotNull(nextLink);

      // Remove the last digit of the continuation token, resulting in the
      // specification of a different iterator hash code.
      URI nextLinkUri = UriComponentsBuilder.fromHttpUrl(nextLink)
          .replaceQueryParam("continuationToken",
              UrlUtil.encodeUrl(continuationToken.substring(0, continuationToken.length() - 1)))
          .build(true).toUri();

      // Request the next page.
      content = controller.perform(getAuthBuilder(get(nextLinkUri)))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      api = mapper.readValue(content, ArtifactPageInfo.class);

      // Get the third group of two artifacts included in the response.
      artifactBuffer = api.getArtifacts();
      assertEquals(2, artifactBuffer.size());
      assertEquals(artifacts.get(4), artifactBuffer.get(0));
      assertEquals(artifacts.get(5), artifactBuffer.get(1));

      // There are more artifacts to be returned.
      continuationToken = api.getPageInfo().getContinuationToken();
      assertNotNull(continuationToken);

      // Get the new iterator hash code.
      Integer newIteratorHashCode = new ArtifactContinuationToken(
	  continuationToken).getIteratorHashCode();
      assertNotNull(newIteratorHashCode);

      // Verify that the new iterator hash code is not the same.
      assertNotEquals(iteratorHashCode, newIteratorHashCode);

      // Get the link needed to get the next page.
      nextLink = api.getPageInfo().getNextLink();
      assertNotNull(nextLink);

      // Request the next page.
      content = controller.perform(getAuthBuilder(get(new URI(nextLink))))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      api = mapper.readValue(content, ArtifactPageInfo.class);

      // Get the fourth group of two artifacts included in the response.
      artifactBuffer = api.getArtifacts();
      assertEquals(2, artifactBuffer.size());
      assertEquals(artifacts.get(6), artifactBuffer.get(0));
      assertEquals(artifacts.get(7), artifactBuffer.get(1));

      // There are more artifacts to be returned.
      continuationToken = api.getPageInfo().getContinuationToken();
      assertNotNull(continuationToken);

      // Verify that the new iterator hash code is the same.
      assertEquals(newIteratorHashCode, new ArtifactContinuationToken(
	  continuationToken).getIteratorHashCode());

      // Get the link needed to get the next page.
      nextLink = api.getPageInfo().getNextLink();
      assertNotNull(nextLink);

      // Request the next page.
      content = controller.perform(getAuthBuilder(get(new URI(nextLink))))
	  .andExpect(status().isOk()).andReturn().getResponse()
	  .getContentAsString();

      // Convert the received content into a page information object.
      api = mapper.readValue(content, ArtifactPageInfo.class);

      // Get the last artifact included in the response.
      artifactBuffer = api.getArtifacts();
      assertEquals(1, artifactBuffer.size());
      assertEquals(artifacts.get(8), artifactBuffer.get(0));

      // There are no more artifacts to be returned.
      assertNull(api.getPageInfo().getContinuationToken());
      assertNull(api.getPageInfo().getNextLink());
      log.debug2("Done");
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

        UnsynchronizedByteArrayOutputStream baos = new UnsynchronizedByteArrayOutputStream();
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

  /**
   * Tests the validation of request limits.
   */
  @Test
  public void testValidateLimit() {
    log.debug2("Invoked");
    assertEquals(1, ServiceImplUtil.validateLimit(null, 1, 10, ""));
    assertEquals(1, ServiceImplUtil.validateLimit(null, 10, 1, ""));
    assertEquals(1, ServiceImplUtil.validateLimit(1, 1, 10, ""));
    assertEquals(5, ServiceImplUtil.validateLimit(5, 1, 10, ""));
    assertEquals(10, ServiceImplUtil.validateLimit(10, 1, 10, ""));
    assertEquals(10, ServiceImplUtil.validateLimit(100, 1, 10, ""));
    assertEquals(10, ServiceImplUtil.validateLimit(100, 50, 10, ""));
    log.debug2("Done");
  }

  /**
   * Provides an authenticated version of a mock request builder, if necessary.
   *
   * @param builder A MockHttpServletRequestBuilder with the unauthenticated
   *                version of the builder.
   * @return a MockHttpServletRequestBuilder with the Authorization header, if
   *         necessary.
   */
  private MockHttpServletRequestBuilder getAuthBuilder(
      MockHttpServletRequestBuilder builder) {
    // Check whether authentication is required.
    if (authHeaderValue != null) {
      // Yes: Add the authentication header.
      return builder.header("Authorization", authHeaderValue);
    }

    // No: Return the passed builder unchanged.
    return builder;
  }
}
