package org.lockss.laaws.rs.configuration;

import org.lockss.laaws.rs.multipart.LockssMultipartResolver;
import org.lockss.util.time.TimeBase;
import org.springframework.boot.web.error.ErrorAttributeOptions;
import org.springframework.boot.web.servlet.error.DefaultErrorAttributes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.multipart.MultipartResolver;

import java.util.Map;

@Configuration
public class MySpringConfiguration {
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
  public MultipartResolver multipartResolver() {
    return new LockssMultipartResolver();
  }
}
