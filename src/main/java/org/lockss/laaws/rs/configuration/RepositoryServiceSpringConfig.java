package org.lockss.laaws.rs.configuration;

import org.apache.commons.io.FileUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.multipart.MultipartResolver;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;
import org.springframework.web.servlet.DispatcherServlet;

/**
 * Spring configuration beans for the Spring-implementation of the LOCKSS Repository Service.
 */
@Configuration
public class RepositoryServiceSpringConfig {

  /**
   * Emits a {@link CommonsMultipartResolver} bean for use in Spring's {@link DispatcherServlet}.
   *
   * See the javadocs of {@link MultipartResolver} and {@link DispatcherServlet} for details.
   *
   * @return A {@link CommonsMultipartResolver} for the Spring-implementation of the LOCKSS Repository Service.
   */
  @Bean
  public CommonsMultipartResolver multipartResolver() {
    CommonsMultipartResolver resolver = new CommonsMultipartResolver();
//    resolver.setMaxInMemorySize((int) (16L * FileUtils.ONE_KB)); // default 10KiB
    return resolver;
  }

}
