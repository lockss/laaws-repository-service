package org.lockss.laaws.rs.configuration;

import org.apache.commons.io.FileUtils;
import org.lockss.config.*;
import org.lockss.log.*;
import org.lockss.util.io.FileUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.FileSystemResource;
import org.springframework.web.multipart.MultipartResolver;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;
import org.springframework.web.servlet.DispatcherServlet;

import java.io.File;
import java.io.IOException;

/**
 * Spring configuration beans for the Spring-implementation of the LOCKSS Repository Service.
 */
@Configuration
public class RepositoryServiceSpringConfig {
  private final static L4JLogger log = L4JLogger.getLogger();

  /** Max size of in-mamory buffering of multipart requests */
  public static String PARAM_MULTIPART_MAX_IN_MEMORY_SIZE =
    org.lockss.config.Configuration.PREFIX + "spring.multipart.maxInMemorySize";
  public static final int DEFAULT_MULTIPART_MAX_IN_MEMORY_SIZE =
    4 * (int)FileUtils.ONE_MB;

  @Autowired
  CommonsMultipartResolver cmResolver;

  /**
   * Emits a {@link CommonsMultipartResolver} bean for use in Spring's {@link DispatcherServlet}.
   * <p>
   * See the javadocs of {@link MultipartResolver} and {@link DispatcherServlet} for details.
   *
   * @return A {@link CommonsMultipartResolver} for the Spring-implementation of the LOCKSS Repository Service.
   */
  @Bean
  public CommonsMultipartResolver multipartResolver() throws IOException {
    CommonsMultipartResolver resolver = new CommonsMultipartResolver();

    return resolver;
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
      if (changedKeys.contains(ConfigManager.PARAM_TMPDIR)) {
	File tmpdir = ConfigManager.getConfigManager().getTmpDir();
	try {
	  log.debug("Setting CommonsMultipartResolver tmpdir to {}", tmpdir);
	  cmResolver.setUploadTempDir(new FileSystemResource(tmpdir));
	} catch (IOException e) {
	  log.warn("Couldn't set CommonsMultipartResolver tmpdir to {}",
		   tmpdir);
	}
      }
      if (changedKeys.contains(PARAM_MULTIPART_MAX_IN_MEMORY_SIZE)) {
	int maxInMem = newConfig.getInt(PARAM_MULTIPART_MAX_IN_MEMORY_SIZE,
					DEFAULT_MULTIPART_MAX_IN_MEMORY_SIZE);
	log.debug("Setting CommonsMultipartResolver maxInMemorySize to {}",
		  maxInMem);
	cmResolver.setMaxInMemorySize(maxInMem);
      }
    }
  }
}
