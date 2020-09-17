package org.lockss.laaws.rs.configuration;

import org.lockss.spring.converter.LockssHttpEntityMethodProcessor;
import org.lockss.util.rest.multipart.MultipartMessageHttpMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.http.converter.support.AllEncompassingFormHttpMessageConverter;
import org.springframework.web.accept.ContentNegotiationManager;
import org.springframework.web.method.support.HandlerMethodReturnValueHandler;
import org.springframework.web.multipart.MultipartResolver;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.mvc.method.annotation.HttpEntityMethodProcessor;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;

import java.util.ArrayList;
import java.util.List;

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
  public CommonsMultipartResolver multipartResolver() {
    CommonsMultipartResolver resolver = new CommonsMultipartResolver();
//    resolver.setMaxInMemorySize((int) (16L * FileUtils.ONE_KB)); // default 10KiB
    return resolver;
  }

  /**
   * Emits a {@link LockssHttpEntityMethodProcessor} bean configured for use in the Repository service.
   *
   * @return An instance of {@link LockssHttpEntityMethodProcessor}.
   */
  public LockssHttpEntityMethodProcessor createLockssHttpEntityMethodProcessor() {
    // Converters for HTTP entity types to be supported by LockssHttpEntityMethodProcessor
    List<HttpMessageConverter<?>> messageConverters = new ArrayList<>();
    messageConverters.add(new MappingJackson2HttpMessageConverter());
    messageConverters.add(new AllEncompassingFormHttpMessageConverter());
    messageConverters.add(new MultipartMessageHttpMessageConverter());

    // Add new LockssHttpEntityMethodProcessor to list from WebMvcConfigurationSupport
    return new LockssHttpEntityMethodProcessor(messageConverters, new ContentNegotiationManager());
  }

  /**
   * Creates an {@link ReplacingRequestMappingHandlerAdapter} that replaces {@link HttpEntityMethodProcessor} with
   * {@link LockssHttpEntityMethodProcessor}.
   *
   * @return An instance of {@link ReplacingRequestMappingHandlerAdapter} having the an updated set of return value
   * handlers.
   */
  @Bean
  public RequestMappingHandlerAdapter createRequestMappingHandlerAdapter() {
    return new ReplacingRequestMappingHandlerAdapter(createLockssHttpEntityMethodProcessor());
  }

  /**
   * Replaces {@link HttpEntityMethodProcessor} in the default list of return value handlers from
   * {@link RequestMappingHandlerAdapter#getDefaultReturnValueHandlers()} with the provided
   * {@link HandlerMethodReturnValueHandler}.
   *
   * FIXME: Could be generalized
   */
  private static class ReplacingRequestMappingHandlerAdapter extends RequestMappingHandlerAdapter {

    // Handle to replacing instance
    private HandlerMethodReturnValueHandler replacingHandler;

    /**
     * Constructor.
     *
     * @param handler The instance of {@link HandlerMethodReturnValueHandler} to replace
     * {@link HttpEntityMethodProcessor} with.
     */
    public ReplacingRequestMappingHandlerAdapter(HandlerMethodReturnValueHandler handler) {
      this.replacingHandler = handler;
    }

    /**
     * Calls {@code super.afterPropertiesSet()} then replaces {@link HttpEntityMethodProcessor} with the provided
     * {@link HandlerMethodReturnValueHandler}.
     */
    @Override
    public void afterPropertiesSet() {
      // Allow default return value handlers to be added
      super.afterPropertiesSet();

      // List to contain new set of handlers
      List<HandlerMethodReturnValueHandler> handlers = new ArrayList<>();

      for (HandlerMethodReturnValueHandler handler : getReturnValueHandlers()) {
        if (handler instanceof HttpEntityMethodProcessor) {
          // Replace HttpEntityMethodProcessor with LockssHttpEntityMethodProcessor
          handlers.add(replacingHandler);
        } else {
          // Pass-through handler
          handlers.add(handler);
        }
      }

      // Set return value handlers
      setReturnValueHandlers(handlers);
    }

  }
}
