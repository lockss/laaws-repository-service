/*

Copyright (c) 2000-2024, Board of Trustees of Leland Stanford Jr. University

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

import jakarta.servlet.MultipartConfigElement;
import org.apache.commons.io.FileUtils;
import org.lockss.config.ConfigManager;
import org.lockss.laaws.rs.multipart.LockssMultipartResolver;
import org.lockss.log.L4JLogger;
import org.lockss.util.time.TimeBase;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.web.servlet.MultipartProperties;
import org.springframework.boot.web.error.ErrorAttributeOptions;
import org.springframework.boot.web.servlet.error.DefaultErrorAttributes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.web.context.request.WebRequest;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Spring configuration beans for the Spring-implementation of the LOCKSS Repository Service.
 */
@Configuration
public class RepositoryServiceSpringConfig {
  private final static L4JLogger log = L4JLogger.getLogger();

  /** Max size of in-memory buffering of multipart requests */
  public static String PARAM_MULTIPART_MAX_IN_MEMORY_SIZE =
    org.lockss.config.Configuration.PREFIX + "spring.multipart.maxInMemorySize";

  public static String DEFAULT_MULTIPART_UPLOAD_DIR = "repo-server";

  public static String PARAM_MULTIPART_UPLOAD_DIR =
      org.lockss.config.Configuration.PREFIX + "spring.multipart.uploadDir";

  public static final int DEFAULT_MULTIPART_MAX_IN_MEMORY_SIZE =
    4 * (int)FileUtils.ONE_MB;

  // FIXME: This is introducing a circular dependency; Spring is now stricter about
  //  such offenses, requiring spring.main.allow-circular-references=true to be set
  @Autowired
  LockssMultipartResolver multipartResolver;

  // FIXME: This was a mistake; revert (and make sure to update our clients)
  @Bean
  public DefaultErrorAttributes errorAttributes() {
    return new DefaultErrorAttributes() {
      @Override
      public Map<String, Object> getErrorAttributes(WebRequest webRequest, ErrorAttributeOptions options) {
        Map<String, Object> attributes = super.getErrorAttributes(webRequest, options);
        attributes.remove("timestamp");
        attributes.put("timestamp", TimeBase.nowMs());
        return attributes;
      }
    };
  }

  @Bean
  public LockssMultipartResolver multipartResolver(ObjectProvider<MultipartProperties> multipartPropsProvider) {
    return new LockssMultipartResolver(multipartPropsProvider.getIfAvailable());
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
          changedKeys.contains(PARAM_MULTIPART_UPLOAD_DIR)) {

        String uploadDir = newConfig.get(PARAM_MULTIPART_UPLOAD_DIR, DEFAULT_MULTIPART_UPLOAD_DIR);
        File tmpdir = new File(ConfigManager.getConfigManager().getTmpDir(), uploadDir);

	try {
	  log.debug("Setting LockssMultipartResolver tmpdir to {}", tmpdir);
	  multipartResolver.setUploadTempDir(tmpdir);
          if (false) throw new IOException();
	} catch (IOException e) {
	  log.warn("Couldn't set LockssMultipartResolver tmpdir to {}",
		   tmpdir);
	}
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
