package org.lockss.laaws.rs.configuration;

import org.apache.commons.io.FileUtils;
import org.lockss.util.io.FileUtil;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.web.multipart.MultipartResolver;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;
import org.springframework.web.servlet.DispatcherServlet;

import java.io.IOException;

/**
 * Spring configuration beans for the Spring-implementation of the LOCKSS Repository Service.
 */
@Configuration
public class RepositoryServiceSpringConfig {

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

    // Set the upload temporary directory
    resolver.setUploadTempDir(new FileSystemResource((FileUtil.createTempDir("multipartResolver", null))));

    // Default is 10KiB which is too small and would cause a lot of temp files to be opened
    resolver.setMaxInMemorySize((int) (4L * FileUtils.ONE_MB));

    return resolver;
  }
}
