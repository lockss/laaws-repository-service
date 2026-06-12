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

package org.lockss.laaws.rs.configuration;

import org.apache.catalina.connector.Connector;
import org.apache.commons.io.FileUtils;
import org.apache.coyote.http11.AbstractHttp11Protocol;
import org.junit.jupiter.api.Assertions;
import org.lockss.config.ConfigManager;
import org.lockss.laaws.rs.multipart.LockssMultipartResolver;
import org.lockss.log.L4JLogger;
import org.lockss.rs.io.storage.ArtifactDataStore;
import org.lockss.rs.io.storage.warc.WarcArtifactDataStore;
import org.lockss.util.Constants;
import org.lockss.util.rest.RestUtil;
import org.mortbay.jetty.servlet.WebApplicationContext;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.web.servlet.MultipartProperties;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.embedded.tomcat.TomcatWebServer;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.web.client.RestTemplate;

import java.io.File;

/**
 * Spring configuration beans for the Spring-implementation of the LOCKSS Repository Service.
 */
@Configuration
public class RepositoryServiceSpringConfig {
  private final static L4JLogger log = L4JLogger.getLogger();

  public static final String MULTIPART_PREFIX =
      org.lockss.config.Configuration.PREFIX + "spring.multipart.";

  /** Max size of in-memory buffering of multipart requests */
  public static String PARAM_MULTIPART_MAX_IN_MEMORY_SIZE =
    MULTIPART_PREFIX + "maxInMemorySize";

  public static String DEFAULT_MULTIPART_UPLOAD_DIR = "repo-server";

  public static String PARAM_MULTIPART_UPLOAD_DIR =
      MULTIPART_PREFIX + "uploadDir";

  public static final String PARAM_MULTIPART_UPLOAD_TIMEOUT =
      MULTIPART_PREFIX + "uploadTimeout";

  public static final long DEFAULT_MULTIPART_UPLOAD_TIMEOUT = 15L * Constants.MINUTE;

  public static final String PARAM_MULTIPART_DISABLE_UPLOAD_TIMEOUT =
      MULTIPART_PREFIX + ".disableUploadTimeout";

  public static final boolean DEFAULT_MULTIPART_DISABLE_UPLOAD_TIMEOUT = false;

  @Autowired public WebServerApplicationContext webCtx;
  @Autowired public ArtifactDataStore ds;
  public static final String CONTENT_MULTIPARTS_DIR = "tmp/multiparts";
  public static final boolean DEFAULT_MULTIPART_USE_CONTENT_FS = true;
  public static final String PARAM_MULTIPART_USE_CONTENT_FS =
            MULTIPART_PREFIX + "useContentFS";

  public static final int DEFAULT_MULTIPART_MAX_IN_MEMORY_SIZE =
    4 * (int)FileUtils.ONE_MB;

  LockssMultipartResolver multipartResolver;
  private long uploadTimeout = DEFAULT_MULTIPART_UPLOAD_TIMEOUT;
  private boolean disableUploadTimeout = DEFAULT_MULTIPART_DISABLE_UPLOAD_TIMEOUT;

  @Bean
  public RestTemplate restTemplate() {
    return RestUtil.getRestTemplate();
  }

  @Bean
  public LockssMultipartResolver multipartResolver(ObjectProvider<MultipartProperties> multipartPropsProvider) {
    multipartResolver = new LockssMultipartResolver(multipartPropsProvider.getIfAvailable());
    return multipartResolver;
  }

  private void setMultipartSettings(Connector connector) {
    AbstractHttp11Protocol<?> proto = (AbstractHttp11Protocol<?>) connector.getProtocolHandler();
    log.debug("Setting disableUploadTimeout to {} from {}",
        disableUploadTimeout, proto.getDisableUploadTimeout());
    log.debug("Setting connectionUploadTimeout to {} from {}",
        uploadTimeout, proto.getConnectionUploadTimeout());

    connector.setProperty("disableUploadTimeout", Boolean.toString(disableUploadTimeout));
    connector.setProperty("connectionUploadTimeout", Long.toString(uploadTimeout));
  }

  @Bean
  public WebServerFactoryCustomizer<TomcatServletWebServerFactory> uploadTimeoutCustomizer() {
    return factory -> factory.addConnectorCustomizers(connector -> {
      setMultipartSettings(connector);
    });
  }

  // When ConfigManager is started, register a config callback to set the
  // multipart resolver tmpdir and maxInMemorySize
  @EventListener
  public void configMgrCreated(ConfigManager.ConfigManagerCreatedEvent event) {
    log.debug2("ConfigManagerCreatedEvent triggered");
    ConfigManager.getConfigManager().registerConfigurationCallback(new ConfigCallback());
  }

  private class ConfigCallback
    implements org.lockss.config.Configuration.Callback {
    public void configurationChanged(org.lockss.config.Configuration newConfig,
				     org.lockss.config.Configuration oldConfig,
				     org.lockss.config.Configuration.Differences changedKeys) {

      if (changedKeys.contains(ConfigManager.PARAM_TMPDIR) ||
          changedKeys.contains(MULTIPART_PREFIX)) {

        String uploadDir =
            newConfig.get(PARAM_MULTIPART_UPLOAD_DIR, DEFAULT_MULTIPART_UPLOAD_DIR);
        boolean useContentFilesystem =
            newConfig.getBoolean(PARAM_MULTIPART_USE_CONTENT_FS, DEFAULT_MULTIPART_USE_CONTENT_FS);

        File tmpDir = useContentFilesystem && (ds != null && ds instanceof WarcArtifactDataStore wads) ?
            new File(wads.getBasePaths()[0].toFile(), CONTENT_MULTIPARTS_DIR) :
            new File(ConfigManager.getConfigManager().getTmpDir(), uploadDir);

        log.debug("Setting multipart upload directory to {}", tmpDir);
        multipartResolver.setUploadTempDir(tmpDir);

        uploadTimeout = newConfig.getTimeInterval(PARAM_MULTIPART_UPLOAD_TIMEOUT,
                                                  DEFAULT_MULTIPART_UPLOAD_TIMEOUT);
        disableUploadTimeout = newConfig.getBoolean(PARAM_MULTIPART_DISABLE_UPLOAD_TIMEOUT,
                                                    DEFAULT_MULTIPART_DISABLE_UPLOAD_TIMEOUT);

        // WebServerApplicationContext webCtx = (WebServerApplicationContext) appCtx;
        TomcatWebServer webServer = (TomcatWebServer) webCtx.getWebServer();
        setMultipartSettings(webServer.getTomcat().getConnector());
      }

      if (changedKeys.contains(PARAM_MULTIPART_MAX_IN_MEMORY_SIZE)) {
	int maxInMem = newConfig.getInt(PARAM_MULTIPART_MAX_IN_MEMORY_SIZE,
					DEFAULT_MULTIPART_MAX_IN_MEMORY_SIZE);
	log.debug("Setting LockssMultipartResolver maxInMemorySize to {}",
		  maxInMem);
	multipartResolver.setMaxInMemorySize(maxInMem);
      }
    }
  }
}
