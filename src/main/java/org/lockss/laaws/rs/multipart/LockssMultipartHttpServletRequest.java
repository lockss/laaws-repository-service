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

/*
 * Copyright 2002-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.lockss.laaws.rs.multipart;

import jakarta.servlet.MultipartConfigElement;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequestWrapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.Part;
import org.apache.catalina.connector.Request;
import org.apache.catalina.core.ApplicationPart;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.util.buf.B2CConverter;
import org.apache.tomcat.util.buf.MessageBytes;
import org.apache.tomcat.util.buf.UDecoder;
import org.apache.tomcat.util.http.Parameters;
import org.apache.tomcat.util.http.Parameters.FailReason;
import org.apache.tomcat.util.http.fileupload.FileItem;
import org.apache.tomcat.util.http.fileupload.FileUpload;
import org.apache.tomcat.util.http.fileupload.MultipartStream.MalformedStreamException;
import org.apache.tomcat.util.http.fileupload.disk.DiskFileItemFactory;
import org.apache.tomcat.util.http.fileupload.impl.InvalidContentTypeException;
import org.apache.tomcat.util.http.fileupload.impl.SizeException;
import org.apache.tomcat.util.http.fileupload.servlet.ServletRequestContext;
import org.lockss.log.L4JLogger;
import org.lockss.util.rest.repo.util.ArtifactConstants;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.lang.Nullable;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.multipart.MultipartException;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.support.AbstractMultipartHttpServletRequest;
import org.springframework.web.multipart.support.StandardMultipartHttpServletRequest;
import org.springframework.web.multipart.support.StandardServletMultipartResolver;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.*;

/**
 * Portions of this class were copied from Spring's {@link StandardMultipartHttpServletRequest} and
 * Tomcat's {@link Request}, and adapted to workaround issues in those implementations that prevent
 * us from supporting features necessary for the operation of LOCKSS Repository Service:
 * <p>
 * 1. Computing part digests should ideally be done as they're read from the network stream, but it
 * is not possible to do this early enough: {@link StandardMultipartHttpServletRequest} wraps an
 * {@link HttpServletRequest} from the servlet framework and calls {@link HttpServletRequest#getParts()}
 * in its parsing of the request. The Tomcat implementation ({@link Request}), is hardcoded to use
 * {@link DiskFileItemFactory} and {@link FileUpload} which write parts (over a size threshold) to
 * temporary files, prior to making them available to controller methods through
 * {@link StandardMultipartHttpServletRequest.StandardMultipartFile}. Jetty appears to use its own
 * multipart parsing logic that is similarly hardcoded. I did not check Undertow.
 * <p>
 * To work around this, Tomcat's {@link Request#getParts()} and {@link Request#parseParts(boolean)}}
 * implementations were copied here and adapted to parse the wrapped {@link HttpServletRequest} using
 * a {@link DigestFileItemFactory}. Since Tomcat's {@link ApplicationPart} does not allow access to
 * its wrapped {@link FileItem}, and there is no {@code getDigest()} or similar in the {@link Part}
 * API, it was also necessary to introduce {@link LockssApplicationPart}. Those are in turn used to
 * construct {@link LockssMultipartFile}s, which are the objects ultimately returned to the REST
 * controller method.
 * <p>
 * 2.Malformed part {@code Content-Type} header handling: We've chosen to transmit the content-type of
 * an artifact in the part's {@code Content-Type} header. Malformed content types were found to cause
 * problems during the serialization of a multipart request (and previously, multipart responses). To
 * work around this, the content type is transmitted via a custom header and falls back to the usual
 * {@code Content-Type} if it is not present. Support for receiving a part employing this workaround and
 * returning the intended, malformed type was added to {@link LockssMultipartFile#getContentType()}.
 *
 * @see StandardMultipartHttpServletRequest
 * @see StandardServletMultipartResolver
 * @see LockssMultipartResolver
 * @see DigestFileItemFactory
 * @see DigestFileItem
 */
public class LockssMultipartHttpServletRequest extends AbstractMultipartHttpServletRequest {
  private static L4JLogger log = L4JLogger.getLogger();

  @Nullable
  private Set<String> multipartParameterNames;
  Collection<Part> parts = null;
  protected Exception partsParseException = null;
  private MultipartConfigElement mce;
  private final Parameters parameters = new Parameters();
  private int maxParameterCount = -1;
  private int maxPostSize = -1;
  private boolean createUploadTargets = true;
  private Charset charset;

  /**
   * Create a new LockssMultipartHttpServletRequest wrapper for the given request,
   * immediately parsing the multipart content.
   *
   * @param request the servlet request to wrap
   * @throws MultipartException if parsing failed
   */
  public LockssMultipartHttpServletRequest(HttpServletRequest request) throws MultipartException {
    this(request, false);
  }

  /**
   * Create a new LockssMultipartHttpServletRequest wrapper for the given request.
   *
   * @param request     the servlet request to wrap
   * @param lazyParsing whether multipart parsing should be triggered lazily on
   *                    first access of multipart files or parameters
   * @throws MultipartException if an immediate parsing attempt failed
   */
  public LockssMultipartHttpServletRequest(HttpServletRequest request, boolean lazyParsing)
      throws MultipartException {


    super(request);
    if (!lazyParsing) {
      parseRequest(request);
    }
  }
  public void setMaxParameterCount(int maxParameterCount) {
    this.maxParameterCount = maxParameterCount;
  }

  public int getMaxParameterCount() {
    return maxParameterCount;
  }
  public void setMaxPostSize(int maxPostSize) {
    this.maxPostSize = maxPostSize;
  }

  public int getMaxPostSize() {
    return maxPostSize;
  }

  private Parameters getParameters() {
    MessageBytes queryMB = MessageBytes.newInstance();
    queryMB.setString(getRequest().getQueryString());
    UDecoder urlDecoder = new UDecoder();

    parameters.setURLDecoder(urlDecoder);
    parameters.setQuery(queryMB);
    parameters.handleQueryParameters();

    return parameters;
  }

  public void setCreateUploadTargets(boolean createUploadTargets) {
    this.createUploadTargets = createUploadTargets;
  }

  public boolean getCreateUploadTargets() {
    return createUploadTargets;
  }

  public LockssMultipartHttpServletRequest setMultipartConfigElement(MultipartConfigElement mce) {
    this.mce = mce;
    return this;
  }

  public MultipartConfigElement getMultipartConfigElement() {
    return mce;
  }

  /**
   * Taken unmodified from {@link org.apache.coyote.Request#getCharset()}.
   */
  public Charset getCharset() throws UnsupportedEncodingException {
    if (charset == null) {
      String characterEncoding = getCharacterEncoding();
      if (characterEncoding != null) {
        charset = B2CConverter.getCharset(characterEncoding);
      }
    }

    return charset;
  }

  /**
   * Taken unmodified from {@link Request#getParts()}.
   */
  @Override
  public Collection<Part> getParts() throws IOException, IllegalStateException, ServletException {
    parseParts(true);

    if (partsParseException != null) {
      if (partsParseException instanceof IOException) {
        throw (IOException) partsParseException;
      } else if (partsParseException instanceof IllegalStateException) {
        throw (IllegalStateException) partsParseException;
      } else if (partsParseException instanceof ServletException) {
        throw (ServletException) partsParseException;
      }
    }

    return parts;
  }

  /**
   * Adapted from Tomcat's {@link Request#parseParts(boolean)}.
   */
  private void parseParts(boolean explicit) throws IOException, ServletException {

    // Return immediately if the parts have already been parsed
    if (parts != null || partsParseException != null) {
      return;
    }

    MultipartConfigElement mce = getMultipartConfigElement();

    int maxParameterCount = getMaxParameterCount();
    Parameters parameters = getParameters();
    parameters.setLimit(maxParameterCount);

    boolean success = false;
    try {
      File location = new File(mce.getLocation());

      if (!location.exists() && getCreateUploadTargets()) {
        log.warn("Temporary directory for parts is missing; will attempt to create it: {}", location);
        if (!location.mkdirs()) {
          log.error("Failed to create temporary directory for parts");
        }
      }

      if (!location.isDirectory()) {
        parameters.setParseFailedReason(FailReason.MULTIPART_CONFIG_INVALID);
        throw new IOException("Upload location invalid: " + location);
      }

      // Create a new file upload handler
      DigestFileItemFactory factory = new DigestFileItemFactory();
      try {
        factory.setRepository(location.getCanonicalFile());
      } catch (IOException ioe) {
        parameters.setParseFailedReason(FailReason.IO_ERROR);
        throw ioe;
      }
      factory.setSizeThreshold(mce.getFileSizeThreshold());

      FileUpload upload = new FileUpload();
      upload.setFileItemFactory(factory);
      upload.setFileSizeMax(mce.getMaxFileSize());
      upload.setSizeMax(mce.getMaxRequestSize());
      if (maxParameterCount > -1) {
        // There is a limit. The limit for parts needs to be reduced by
        // the number of parameters we have already parsed.
        // Must be under the limit else parsing parameters would have
        // triggered an exception.
        upload.setFileCountMax(maxParameterCount - parameters.size());
      }

      parts = new ArrayList<>();
      try {
        List<FileItem> items = upload.parseRequest(new ServletRequestContext(getRequest()));
        int maxPostSize = getMaxPostSize();
        int postSize = 0;
        Charset charset = getCharset();
        for (FileItem item : items) {
          ApplicationPart part = new LockssApplicationPart((DigestFileItem) item, location);
          parts.add(part);
          if (part.getSubmittedFileName() == null) {
            String name = part.getName();
            if (maxPostSize >= 0) {
              // Have to calculate equivalent size. Not completely
              // accurate but close enough.
              postSize += name.getBytes(charset).length;
              // Equals sign
              postSize++;
              // Value length
              postSize += part.getSize();
              // Value separator
              postSize++;
              if (postSize > maxPostSize) {
                parameters.setParseFailedReason(FailReason.POST_TOO_LARGE);
                throw new IllegalStateException("maxPostSized exceeded");
              }
            }
            String value = null;
            try {
              value = part.getString(charset.name());
            } catch (UnsupportedEncodingException uee) {
              // Not possible
            }
            parameters.addParameter(name, value);
          }
        }

        success = true;
      } catch (InvalidContentTypeException e) {
        parameters.setParseFailedReason(FailReason.INVALID_CONTENT_TYPE);
        partsParseException = new ServletException(e);
      } catch (SizeException e) {
        parameters.setParseFailedReason(FailReason.POST_TOO_LARGE);
//        checkSwallowInput();
        partsParseException = new IllegalStateException(e);
      } catch (IOException e) {
        parameters.setParseFailedReason(FailReason.IO_ERROR);
        partsParseException = e;
        Throwable ppeCause = partsParseException.getCause();

        if (ppeCause instanceof MalformedStreamException) {
          String clientStr = "[client: " + getRemoteHost() + ", clientIP: " + getRemoteAddr() +"]";
          String errMsg = "Error processing malformed multipart request from client "
              + clientStr + ": " + ppeCause.getMessage();

          log.error(errMsg);

          partsParseException = getQuietMultipartStreamException((MalformedStreamException) ppeCause);
        }
      } catch (IllegalStateException e) {
        // addParameters() will set parseFailedReason
//        checkSwallowInput();
        partsParseException = e;
      }
    } finally {
      // This might look odd but is correct. setParseFailedReason() only
      // sets the failure reason if none is currently set. This code could
      // be more efficient but it is written this way to be robust with
      // respect to changes in the remainder of the method.
      if (partsParseException != null || !success) {
        parameters.setParseFailedReason(FailReason.UNKNOWN);
      }
    }
  }

  /**
   * Given a {@link MalformedStreamException}, return one without a stacktrace, unless logging
   * at {@code TRACE} level.
   */
  private MalformedStreamException getQuietMultipartStreamException(MalformedStreamException mse) {
    if (!log.isTraceEnabled()) {
      return new MalformedStreamException(mse.getMessage()) {
        @Override
        public synchronized Throwable fillInStackTrace() {
          return this;
        }
      };
    }

    return mse;
  }

  /**
   * Adapted from {@link StandardMultipartHttpServletRequest#parseRequest(HttpServletRequest)}.
   * Modified to construct {@link LockssMultipartFile} objects from {@link LockssApplicationPart}
   * objects created from parsing the multipart request.
   */
  private void parseRequest(HttpServletRequest request) {
    try {
      Collection<Part> parts = getParts();
      this.multipartParameterNames = new LinkedHashSet<>(parts.size());
      MultiValueMap<String, MultipartFile> files = new LinkedMultiValueMap<>(parts.size());
      for (Part part : parts) {
        String headerValue = part.getHeader(HttpHeaders.CONTENT_DISPOSITION);
        ContentDisposition disposition = ContentDisposition.parse(headerValue);
        String filename = disposition.getFilename();
        if (filename != null) {
          files.add(part.getName(), new LockssMultipartFile((LockssApplicationPart) part, filename));
        } else {
          this.multipartParameterNames.add(part.getName());
        }
      }
      setMultipartFiles(files);
    } catch (Throwable ex) {
      handleParseFailure(ex);
    }
  }

  /**
   * Taken unmodified from {@link StandardMultipartHttpServletRequest#handleParseFailure(Throwable)}.
   */
  protected void handleParseFailure(Throwable ex) {
    // MaxUploadSizeExceededException ?
    Throwable cause = ex;
    do {
      String msg = cause.getMessage();
      if (msg != null) {
        msg = msg.toLowerCase();
        if (msg.contains("exceed") && (msg.contains("size") || msg.contains("length"))) {
          throw new MaxUploadSizeExceededException(-1, ex);
        }
      }
      cause = cause.getCause();
    }
    while (cause != null);

    // General MultipartException
    throw new MultipartException("Failed to parse multipart servlet request", ex);
  }

  /**
   * Taken unmodified from {@link StandardMultipartHttpServletRequest#initializeMultipart()}.
   */
  @Override
  protected void initializeMultipart() {
    parseRequest(getRequest());
  }

  /**
   * Taken unmodified from {@link StandardMultipartHttpServletRequest#getParameterNames()}.
   */
  @Override
  public Enumeration<String> getParameterNames() {
    if (this.multipartParameterNames == null) {
      initializeMultipart();
    }
    if (this.multipartParameterNames.isEmpty()) {
      return super.getParameterNames();
    }

    // Servlet getParameterNames() not guaranteed to include multipart form items
    // (e.g. on WebLogic 12) -> need to merge them here to be on the safe side
    Set<String> paramNames = new LinkedHashSet<>();
    Enumeration<String> paramEnum = super.getParameterNames();
    while (paramEnum.hasMoreElements()) {
      paramNames.add(paramEnum.nextElement());
    }
    paramNames.addAll(this.multipartParameterNames);
    return Collections.enumeration(paramNames);
  }

  /**
   * Adapted from {@link ServletRequestWrapper#getParameterValues(String)}, a parent
   * class of {@link StandardMultipartHttpServletRequest}.
   */
  @Override
  public String[] getParameterValues(String name) {
    return parameters.getParameterValues(name);
  }

  /**
   * Taken unmodified from {@link StandardMultipartHttpServletRequest#getParameterMap()}.
   */
  @Override
  public Map<String, String[]> getParameterMap() {
    if (this.multipartParameterNames == null) {
      initializeMultipart();
    }
    if (this.multipartParameterNames.isEmpty()) {
      return super.getParameterMap();
    }

    // Servlet getParameterMap() not guaranteed to include multipart form items
    // (e.g. on WebLogic 12) -> need to merge them here to be on the safe side
    Map<String, String[]> paramMap = new LinkedHashMap<>(super.getParameterMap());
    for (String paramName : this.multipartParameterNames) {
      if (!paramMap.containsKey(paramName)) {
        paramMap.put(paramName, getParameterValues(paramName));
      }
    }
    return paramMap;
  }

  /**
   * Taken unmodified from {@link StandardMultipartHttpServletRequest#getMultipartContentType(String)}.
   */
  @Override
  public String getMultipartContentType(String paramOrFileName) {
    try {
      Part part = getPart(paramOrFileName);
      return (part != null ? part.getContentType() : null);
    } catch (Throwable ex) {
      throw new MultipartException("Could not access multipart servlet request", ex);
    }
  }

  /**
   * Taken unmodified from {@link StandardMultipartHttpServletRequest#getMultipartHeaders(String)}.
   */
  @Override
  public HttpHeaders getMultipartHeaders(String paramOrFileName) {
    try {
      Part part = getPart(paramOrFileName);
      if (part != null) {
        HttpHeaders headers = new HttpHeaders();
        for (String headerName : part.getHeaderNames()) {
          headers.put(headerName, new ArrayList<>(part.getHeaders(headerName)));
        }
        return headers;
      } else {
        return null;
      }
    } catch (Throwable ex) {
      throw new MultipartException("Could not access multipart servlet request", ex);
    }
  }

  /**
   * Taken unmodified from {@link Request#getPart(String)}.
   */
  @Override
  public Part getPart(String name) throws IOException, IllegalStateException, ServletException {
    for (Part part : getParts()) {
      if (name.equals(part.getName())) {
        return part;
      }
    }
    return null;
  }

  /**
   * LOCKSS implementation of a Spring {@link MultipartFile} adapter, wrapping a
   * LOCKSS Servlet Part object (see {@link LockssApplicationPart}). The goal of
   * this class is to expose the part digest and X-Lockss-Content-Type header if
   * present.
   *
   * Adapted from {@link StandardMultipartHttpServletRequest.StandardMultipartFile}.
   */
  @SuppressWarnings("serial")
  public static class LockssMultipartFile implements MultipartFile, Serializable {

    private final LockssApplicationPart part;

    private final String filename;

    public LockssMultipartFile(LockssApplicationPart part, String filename) {
      this.part = part;
      this.filename = filename;
    }

    public MessageDigest getDigest() {
      return part.getDigest();
    }

    @Override
    public String getName() {
      return this.part.getName();
    }

    @Override
    public String getOriginalFilename() {
      return this.filename;
    }

    @Override
    public String getContentType() {
      String contentType =
          this.part.getHeader(ArtifactConstants.X_LOCKSS_CONTENT_TYPE);

      return StringUtils.isEmpty(contentType) ?
          this.part.getContentType() : contentType;
    }

    @Override
    public boolean isEmpty() {
      return (this.part.getSize() == 0);
    }

    @Override
    public long getSize() {
      return this.part.getSize();
    }

    @Override
    public byte[] getBytes() throws IOException {
      return FileCopyUtils.copyToByteArray(this.part.getInputStream());
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return this.part.getInputStream();
    }

    @Override
    public void transferTo(File dest) throws IOException, IllegalStateException {
      this.part.write(dest.getPath());
      if (dest.isAbsolute() && !dest.exists()) {
        // Servlet Part.write is not guaranteed to support absolute file paths:
        // may translate the given path to a relative location within a temp dir
        // (e.g. on Jetty whereas Tomcat and Undertow detect absolute paths).
        // At least we offloaded the file from memory storage; it'll get deleted
        // from the temp dir eventually in any case. And for our user's purposes,
        // we can manually copy it to the requested location as a fallback.
        FileCopyUtils.copy(this.part.getInputStream(), Files.newOutputStream(dest.toPath()));
      }
    }

    @Override
    public void transferTo(Path dest) throws IOException, IllegalStateException {
      FileCopyUtils.copy(this.part.getInputStream(), Files.newOutputStream(dest));
    }
  }

  /**
   * Extends {@link ApplicationPart} to expose the digest from our {@link DigestFileItem}.
   */
  private static class LockssApplicationPart extends ApplicationPart {
    private final DigestFileItem fileItem;

    public LockssApplicationPart(DigestFileItem fileItem, File location) {
      super(fileItem, location);
      this.fileItem = fileItem;
    }

    public MessageDigest getDigest() {
      return fileItem.getDigest();
    }
  }
}
