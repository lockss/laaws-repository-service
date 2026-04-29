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

package org.lockss.laaws.rs.multipart;

import jakarta.servlet.MultipartConfigElement;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.catalina.connector.Connector;
import org.lockss.laaws.rs.configuration.RepositoryServiceSpringConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.servlet.autoconfigure.MultipartProperties;
import org.springframework.boot.tomcat.TomcatWebServer;
import org.springframework.boot.web.server.WebServer;
import org.springframework.boot.servlet.MultipartConfigFactory;
import org.springframework.boot.web.server.servlet.context.ServletWebServerApplicationContext;
import org.springframework.util.StringUtils;
import org.springframework.util.unit.DataSize;
import org.springframework.web.multipart.MultipartException;
import org.springframework.web.multipart.MultipartHttpServletRequest;
import org.springframework.web.multipart.support.AbstractMultipartHttpServletRequest;
import org.springframework.web.multipart.support.StandardServletMultipartResolver;

import java.io.File;
import java.io.IOException;

/**
 * The {@link LockssMultipartResolver} overrides
 * {@link StandardServletMultipartResolver#resolveMultipart(HttpServletRequest)}
 * to return {@link LockssMultipartHttpServletRequest} objects with our customized
 * wrapping and handling of multipart {@link HttpServletRequest} objects. It also
 * handles parameters related to its multipart processing, such as the temporary
 * directory used, maximum in-memory threshold, etc.
 * <p>
 * See {@link LockssMultipartHttpServletRequest} for additional details. The
 * {@link LockssMultipartResolver} is constructed as a bean in
 * {@link RepositoryServiceSpringConfig}.
 */
public class LockssMultipartResolver extends StandardServletMultipartResolver {
  private final MultipartConfigFactory multipartConfigFactory;

  // Spring 7: resolveLazily controls whether multipart parsing is deferred
  private boolean resolveLazily = false;

  // Spring 7: strictServletCompliance controls multipart content-type matching
  private boolean strictServletCompliance = false;

//  @Autowired
//  ServletWebServerApplicationContext context;

  public LockssMultipartResolver(MultipartProperties props) {
    multipartConfigFactory = new MultipartConfigFactory();

    if (props != null) {
      multipartConfigFactory.setLocation(props.getLocation());
      multipartConfigFactory.setMaxFileSize(props.getMaxFileSize());
      multipartConfigFactory.setMaxRequestSize(props.getMaxRequestSize());
      multipartConfigFactory.setFileSizeThreshold(props.getFileSizeThreshold());
    }
  }

  /**
   * Spring 7: set whether to resolve the multipart request lazily at the time of
   * file or parameter access.
   */
  public void setResolveLazily(boolean resolveLazily) {
    this.resolveLazily = resolveLazily;
  }

  public boolean isResolveLazily() {
    return resolveLazily;
  }

  /**
   * Spring 7: set whether to comply strictly with the Servlet spec, requiring
   * {@code multipart/form-data} content type. When {@code false} (the default),
   * any {@code multipart/} content type is accepted.
   */
  public void setStrictServletCompliance(boolean strictServletCompliance) {
    this.strictServletCompliance = strictServletCompliance;
  }

  public boolean isStrictServletCompliance() {
    return strictServletCompliance;
  }

  /**
   * Spring 7: check content type with strictServletCompliance support.
   * When strict, only {@code multipart/form-data} is accepted; otherwise
   * any {@code multipart/*} is accepted.
   */
  @Override
  public boolean isMultipart(HttpServletRequest request) {
    return StringUtils.startsWithIgnoreCase(request.getContentType(),
        this.strictServletCompliance ? "multipart/form-data" : "multipart/");
  }

  @Override
  public MultipartHttpServletRequest resolveMultipart(HttpServletRequest request) throws MultipartException {
    MultipartConfigElement mce = getMultipartConfigElement();
    // Note: always construct lazily first, then set MCE, then trigger parsing if
    // resolveLazily is false. The MCE must be set before parsing can proceed.
    LockssMultipartHttpServletRequest lockssMultipartRequest =
        new LockssMultipartHttpServletRequest(request, true)
            .setMultipartConfigElement(mce);

//    WebServer ws = context.getWebServer();
//    if (ws instanceof TomcatWebServer) {
//      TomcatWebServer tws = (TomcatWebServer) context.getWebServer();
//      Connector connector = tws.getTomcat().getConnector();
//
////       lockssMultipartRequest.setMaxPostSize(mce.getMaxRequestSize());
//      lockssMultipartRequest.setMaxPostSize(connector.getMaxPostSize());
//      lockssMultipartRequest.setMaxParameterCount(connector.getMaxParameterCount());
//    }

    return lockssMultipartRequest;
  }

  /**
   * Spring 7: cleanup multipart resources. Iterates over parts and deletes them.
   */
  @Override
  public void cleanupMultipart(MultipartHttpServletRequest request) {
    if (request instanceof AbstractMultipartHttpServletRequest abstractRequest) {
      if (!abstractRequest.isResolved()) {
        return;
      }
    }
    try {
      for (var part : request.getParts()) {
        part.delete();
      }
    } catch (Throwable ex) {
      org.apache.commons.logging.LogFactory.getLog(getClass())
          .warn("Failed to perform cleanup of multipart items", ex);
    }
  }

  public MultipartConfigElement getMultipartConfigElement() {
    return multipartConfigFactory.createMultipartConfig();
  }

  public void setUploadTempDir(File uploadTempDir) {
    multipartConfigFactory.setLocation(uploadTempDir.getAbsolutePath());
  }

  public void setMaxInMemorySize(int maxInMem) {
    multipartConfigFactory.setFileSizeThreshold(DataSize.ofBytes(maxInMem));
  }
}
