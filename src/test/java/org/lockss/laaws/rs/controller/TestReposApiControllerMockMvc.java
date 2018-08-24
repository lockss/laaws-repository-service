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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.tools.javac.util.ArrayUtils;
import com.sun.tools.javac.util.List;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.lockss.laaws.rs.api.CollectionsApi;
import org.lockss.laaws.rs.api.CollectionsApiController;
import org.lockss.laaws.rs.api.RepositoryServiceControllerAdvice;
import org.lockss.laaws.rs.converters.ArtifactDataMessageConverter;
import org.lockss.laaws.rs.core.LockssRepository;
import org.lockss.laaws.rs.core.VolatileLockssRepository;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.laaws.rs.model.ArtifactData;
import org.lockss.laaws.rs.model.ArtifactIdentifier;
import org.lockss.laaws.rs.model.RepositoryArtifactMetadata;
import org.lockss.util.test.LockssTestCase5;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.MockMvcBuilderCustomizer;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.json.JacksonTester;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.*;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.springframework.test.web.servlet.setup.ConfigurableMockMvcBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.test.web.servlet.setup.MockMvcConfigurer;
import org.springframework.test.web.servlet.setup.MockMvcConfigurerAdapter;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.method.annotation.ExceptionHandlerMethodResolver;
import org.springframework.web.method.support.HandlerMethodReturnValueHandler;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.servlet.mvc.method.annotation.ExceptionHandlerExceptionResolver;
import org.springframework.web.servlet.mvc.method.annotation.ServletInvocableHandlerMethod;

import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.UUID;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

@RunWith(SpringJUnit4ClassRunner.class)
//@SpringBootTest
@WebMvcTest
@AutoConfigureMockMvc(secure = false)
@WebAppConfiguration
public class TestReposApiControllerMockMvc {
  @Autowired
  private MockMvc mockMvc;

  @MockBean
  private LockssRepository repository;

  @Autowired
  private CollectionsApiController controller;

  @Autowired
  ApplicationContext appContext;

  @Autowired
  private WebApplicationContext wac;

  @Before
  public void setup() {
//    this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
//        .setHandlerExceptionResolvers(createExceptionResolver())
//        .setMessageConverters(new MappingJackson2HttpMessageConverter())
//        .build();
  }

  private ExceptionHandlerExceptionResolver createExceptionResolver() {
    ExceptionHandlerExceptionResolver exceptionResolver = new ExceptionHandlerExceptionResolver() {
      protected ServletInvocableHandlerMethod getExceptionHandlerMethod(HandlerMethod handlerMethod, Exception exception) {
        Method method = new ExceptionHandlerMethodResolver(RepositoryServiceControllerAdvice.class).resolveMethod(exception);
        return new ServletInvocableHandlerMethod(new RepositoryServiceControllerAdvice(), method);
      }
    };

    java.util.List<HttpMessageConverter<?>> converters = exceptionResolver.getMessageConverters();
    converters.add(new MappingJackson2HttpMessageConverter());
    exceptionResolver.setMessageConverters(converters);

    exceptionResolver.afterPropertiesSet();
    exceptionResolver.setApplicationContext(appContext);
    return exceptionResolver;
  }

  @EnableWebMvc
  @TestConfiguration
  public static class TestMockMvcConfig {
    @Bean
    public WebMvcConfigurerAdapter something() {
      return new WebMvcConfigurerAdapter() {
//        @Override
//        public void addReturnValueHandlers(java.util.List<HandlerMethodReturnValueHandler> returnValueHandlers) {
//          super.addReturnValueHandlers(returnValueHandlers);
//        }
//
//        @Override
//        public void configureMessageConverters(java.util.List<HttpMessageConverter<?>> converters) {
////          converters.add(new MappingJackson2HttpMessageConverter());
//          super.configureMessageConverters(converters);
//        }
//
        @Override
        public void extendMessageConverters(java.util.List<HttpMessageConverter<?>> converters) {
          converters.add(new ArtifactDataMessageConverter());
          super.configureMessageConverters(converters);
        }
//
//        @Override
//        public void configureHandlerExceptionResolvers(java.util.List<HandlerExceptionResolver> exceptionResolvers) {
//          super.configureHandlerExceptionResolvers(exceptionResolvers);
//        }
//
//        @Override
//        public void extendHandlerExceptionResolvers(java.util.List<HandlerExceptionResolver> exceptionResolvers) {
//          ExceptionHandlerExceptionResolver exceptionResolver = new ExceptionHandlerExceptionResolver() {
//            protected ServletInvocableHandlerMethod getExceptionHandlerMethod(HandlerMethod handlerMethod, Exception exception) {
//              Method method = new ExceptionHandlerMethodResolver(RepositoryServiceControllerAdvice.class).resolveMethod(exception);
//              return new ServletInvocableHandlerMethod(new RepositoryServiceControllerAdvice(), method);
//            }
//          };
//
//          java.util.List<HttpMessageConverter<?>> converters = exceptionResolver.getMessageConverters();
//          converters.add(new MappingJackson2HttpMessageConverter());
//          exceptionResolver.setMessageConverters(converters);
//
//
//          exceptionResolvers.add(exceptionResolver);
//          super.extendHandlerExceptionResolvers(exceptionResolvers);
//        }
      };
    }
  }


  /*
  private static class ReadiableVolatileLockssRepository extends VolatileLockssRepository {
    boolean ready = false;

    public void setReady(boolean ready) {
      this.ready = ready;
    }

    @Override
    public boolean isReady() {
      return ready;
    }
  }

  @TestConfiguration
  public static class TestLockssRepositoryConfiguration {
    @Bean
    public LockssRepository createRepository() {
      return new ReadiableVolatileLockssRepository();
    }
  }
  */

  @Test
  public void testGetCollections() throws Exception {
    Artifact a1 = repository.addArtifact(randomArtifactData());
    repository.commitArtifact(a1);

    // Check that the controller returns 503 from this endpoint if the repository is not ready
    when(repository.isReady()).thenReturn(false);
    mockMvc.perform(get("/collections"))
        .andExpect(status().isServiceUnavailable());

    // Proceed with a ready internal repository
    when(repository.isReady()).thenReturn(true);

    // Check empty
    when(repository.getCollectionIds()).thenReturn(Collections.EMPTY_LIST);
    mockMvc.perform(get("/collections"))
        .andExpect(status().isOk())
        .andExpect(content().json("[]"));

    // Check populated
    when(repository.getCollectionIds()).thenReturn(List.of("c1","c2"));
    mockMvc.perform(get("/collections"))
        .andExpect(status().isOk())
        .andExpect(content().json("[\"c1\",\"c2\"]"));
  }

  @Test
  public void testDeleteArtifact() throws Exception {
    // Check that the controller returns 503 from this endpoint if the repository is not ready
    when(repository.isReady()).thenReturn(false);
    mockMvc.perform(delete("/collections/x/artifacts/y"))
        .andExpect(status().isServiceUnavailable());

    // Proceed with a ready internal repository
    when(repository.isReady()).thenReturn(true);

    // Attempt delete on non-existent artifact
    mockMvc.perform(delete("/collections/x/artifacts/y"))
        .andExpect(status().isNotFound());

    // Attempt delete on existent artifact
    when(repository.artifactExists("x", "y")).thenReturn(true);
    doNothing().when(repository).deleteArtifact("x","y");
    mockMvc.perform(delete("/collections/x/artifacts/y"))
        .andExpect(status().isOk());

    // Raise IOException when attempt to delete an artifact
    doThrow(IOException.class).when(repository).deleteArtifact("x","y");
    mockMvc.perform(delete("/collections/x/artifacts/y"))
        .andExpect(status().isInternalServerError());
  }

  @Test
  public void testGetArtifact() throws Exception {
    // Check that the controller returns 503 from this endpoint if the repository is not ready
    when(repository.isReady()).thenReturn(false);
    mockMvc.perform(get("/collections/x/artifacts/y"))
        .andExpect(status().isServiceUnavailable());

    // Proceed with a ready internal repository
    when(repository.isReady()).thenReturn(true);

    // Attempt get on non-existent artifact
    when(repository.artifactExists("x", "y")).thenReturn(false);
    mockMvc.perform(get("/collections/x/artifacts/y"))
        .andExpect(status().isNotFound());

    // Create a fake artifact
    ArtifactData ad1 = randomArtifactData();
    ad1.getIdentifier().setId("y");
    ad1.setContentDigest("SHA1");
    ad1.setContentLength(5);
    ad1.setRepositoryMetadata(new RepositoryArtifactMetadata(ad1.getIdentifier(), true, false));


    // Attempt delete on existent artifact
//    when(repository.isReady()).thenReturn(true);
    when(repository.artifactExists("x", "y")).thenReturn(true);
    when(repository.getArtifactData("x","y")).thenReturn(ad1);
    mockMvc.perform(get("/collections/x/artifacts/y").accept(MediaType.parseMediaType("application/http")))
        .andDo(print())
        .andExpect(status().isOk())
    .andExpect(content().bytes("hello".getBytes()));

  }

  @Test
  public void testPostArtifact() throws Exception {
    if (false) {
      // TODO: Make an artifact

      // TODO: Encode an artifact as a HTTP POST request body
      byte[] m_encodedArtifact = "A byte[] encoding of an artifact which will be transmitted as the request body of a HTTP POST".getBytes();

      // Call the real addArtifact() method if it is passed an ArtifactData
      doCallRealMethod().when(repository.addArtifact(isA(ArtifactData.class)));

      // Add an artifact via the controller
      ResultActions result = mockMvc.perform(post("/collections/x/artifacts").contentType(MediaType.parseMediaType("application/http")).content(m_encodedArtifact))
          .andExpect(status().isOk());

      // Check that the artifact exists in the internal repository
      result.andExpect(mvcResult -> {
        byte[] responseBody = mvcResult.getResponse().getContentAsByteArray();

        // Extract the collection and artifact ID from the controller's response
        String artifactId = "ok";
        String collectionId = "ok";

        // Check that the artifact exists in the internal repository
//        assertTrue(repository.artifactExists(collectionId, artifactId));
      });

    }
  }

  private ArtifactData randomArtifactData() {
    StatusLine status = new BasicStatusLine(new ProtocolVersion("HTTP", 1,1), 200, "OK");

    return makeArtifactData(
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString(),
        (long) (Math.random() * Long.MAX_VALUE),
        UUID.randomUUID().toString().getBytes(),
//        "hello world".getBytes(),
        status
    );
  }

  private ArtifactData randomArtifactData(String collection, String auid) {
    StatusLine status = new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK");

    return makeArtifactData(
        collection,
        auid,
        UUID.randomUUID().toString(),
        (long) (Math.random() * Long.MAX_VALUE),
        UUID.randomUUID().toString().getBytes(),
        status
    );
  }

  private ArtifactData makeArtifactData(String collection, String auid, String url, long version, byte[] data, StatusLine status) {
    ArtifactIdentifier id = new ArtifactIdentifier(collection, auid, url, (int)version);
    InputStream inputStream = new ByteArrayInputStream(data);
    return new ArtifactData(id, null, inputStream, status);
  }
}
