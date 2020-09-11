package org.lockss.laaws.rs.configuration;

import org.lockss.spring.converter.LockssHttpEntityMethodProcessor;
import org.lockss.util.rest.multipart.MultipartMessageHttpMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.http.converter.support.AllEncompassingFormHttpMessageConverter;
import org.springframework.web.method.support.HandlerMethodReturnValueHandler;
import org.springframework.web.multipart.MultipartResolver;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;
import org.springframework.web.servlet.mvc.method.annotation.HttpEntityMethodProcessor;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * Spring configuration beans for the Spring-implementation of the LOCKSS Repository Service.
 */
@Configuration
public class RepositoryServiceSpringConfig extends WebMvcConfigurationSupport {

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

  /**
   * Adds the {@link LockssHttpEntityMethodProcessor} to the list of return-value handlers used by
   * {@link WebMvcConfigurationSupport}.
   *
   * Note: The order in which this is invoked is sensitive! {@link LockssHttpEntityMethodProcessor} must come earlier
   * in the list than (or replace) {@link HttpEntityMethodProcessor}.
   *
   * @param returnValueHandlers The {@link List<HandlerMethodReturnValueHandler>} to add an instance of
   * {@link LockssHttpEntityMethodProcessor} to.
   */
  @Override
  public void addReturnValueHandlers(final List<HandlerMethodReturnValueHandler> returnValueHandlers) {
    // Converters for HTTP entity types to be supported by LockssHttpEntityMethodProcessor
    List<HttpMessageConverter<?>> messageConverters = new ArrayList<>();
    messageConverters.add(new MappingJackson2HttpMessageConverter());
    messageConverters.add(new AllEncompassingFormHttpMessageConverter());
    messageConverters.add(new MultipartMessageHttpMessageConverter());

    // Add new LockssHttpEntityMethodProcessor to list from WebMvcConfigurationSupport
    returnValueHandlers.add(new LockssHttpEntityMethodProcessor(messageConverters, mvcContentNegotiationManager()));
  }

  /**
   * Taken from {@link WebMvcConfigurationSupport#requestMappingHandlerAdapter()}.
   *
   * The purpose for this override is to use setReturnValueHandlers instead of setCustomReturnValueHandlers. The latter
   * appends custom handlers to the end of a default list, which doesn't work for custom handlers that are supposed
   * to replace (or come earlier in the list than) a handler provided by default.
   *
   * @return
   */
  @Override
  @Bean
  public RequestMappingHandlerAdapter requestMappingHandlerAdapter() {
    RequestMappingHandlerAdapter adapter = createRequestMappingHandlerAdapter();

    adapter.setContentNegotiationManager(mvcContentNegotiationManager());
    adapter.setMessageConverters(getMessageConverters());
    adapter.setWebBindingInitializer(getConfigurableWebBindingInitializer());
    adapter.setCustomArgumentResolvers(getArgumentResolvers());

    adapter.setReturnValueHandlers(getReturnValueHandlers());

    // FIXME: Incomplete - see super method implementation

    return adapter;
  }
}
