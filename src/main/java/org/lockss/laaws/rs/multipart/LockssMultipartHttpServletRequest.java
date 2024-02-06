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
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.Part;
import org.apache.catalina.core.ApplicationPart;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.util.buf.MessageBytes;
import org.apache.tomcat.util.http.Parameters;
import org.apache.tomcat.util.http.fileupload.FileItem;
import org.apache.tomcat.util.http.fileupload.FileUpload;
import org.apache.tomcat.util.http.fileupload.disk.DiskFileItemFactory;
import org.apache.tomcat.util.http.fileupload.impl.InvalidContentTypeException;
import org.apache.tomcat.util.http.fileupload.impl.SizeException;
import org.apache.tomcat.util.http.fileupload.servlet.ServletRequestContext;
import org.lockss.util.FileUtil;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * This class is a copy of Spring's {@link StandardMultipartHttpServletRequest}, modified to return
 * {@link LockssMultipartFile} parts. This was necessary because the {@link MultipartFile} API does
 * not expose the part headers but we need {@link MultipartFile#getContentType()} calls to return
 * the {@code X-Lockss-Content-Type} header if present, otherwise fallback to {@code Content-Type}.
 *
 * @see StandardMultipartHttpServletRequest
 * @see StandardServletMultipartResolver
 * @see LockssMultipartResolver
 */
public class LockssMultipartHttpServletRequest extends AbstractMultipartHttpServletRequest {

  @Nullable
  private Set<String> multipartParameterNames;

  private HttpServletRequest request;

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
   * @since 3.2.9
   */
  public LockssMultipartHttpServletRequest(HttpServletRequest request, boolean lazyParsing)
      throws MultipartException {

    super(request);
    this.request = request;
    if (!lazyParsing) {
      parseRequest(request);
    }
  }

  @Override
  public String[] getParameterValues(String name) {
    return parameters.getParameterValues(name);
  }

  Collection<Part> parts = null;
  Parameters parameters;

  @Override
  public Collection<Part> getParts() throws IOException, ServletException {

    File defaultLocation = FileUtil.createTempDir("multipart", null);

    MultipartConfigElement mce =
        new MultipartConfigElement(defaultLocation.getAbsolutePath());

    int maxParameterCount = 10;
    int maxPostSize = 1024*1024*1024;

    if (this.parts == null) {
      parameters = new Parameters();
      MessageBytes queryMB = MessageBytes.newInstance();
      queryMB.setString(request.getQueryString());
      parameters.setQuery(queryMB);
      parameters.handleQueryParameters();

      parts = parseParts(parameters, mce, defaultLocation,
          true,
          maxParameterCount,
          maxPostSize,
          StandardCharsets.UTF_8);
    }

    return parts;
  }

  private Collection<Part> parseParts(
      Parameters parameters,
      MultipartConfigElement mce,
      File defaultLocation,
      boolean createUploadTargets,
      int maxParameterCount,
      int maxPostSize,
      Charset charset
  ) throws IOException, ServletException {
    parameters.setLimit(maxParameterCount);

    Collection<Part>parts = new ArrayList<>();
    boolean success = false;
    try {
      File location;
      String locationStr = mce.getLocation();
      if (locationStr == null || locationStr.length() == 0) {
        location = defaultLocation;
      } else {
        // If relative, it is relative to TEMPDIR
        location = new File(locationStr);
        if (!location.isAbsolute()) {
          location = new File(defaultLocation, locationStr).getAbsoluteFile();
        }
      }

      if (!location.exists() && createUploadTargets) {
//        log.warn(sm.getString("coyoteRequest.uploadCreate", location.getAbsolutePath(),
//            getMappingData().wrapper.getName()));
        if (!location.mkdirs()) {
//          log.warn(sm.getString("coyoteRequest.uploadCreateFail", location.getAbsolutePath()));
        }
      }

      if (!location.isDirectory()) {
        parameters.setParseFailedReason(Parameters.FailReason.MULTIPART_CONFIG_INVALID);
        throw new IOException("Upload location invalid: " + location);
      }

      // Create a new file upload handler
      DiskFileItemFactory factory = new DiskFileItemFactory();
      try {
        factory.setRepository(location.getCanonicalFile());
      } catch (IOException ioe) {
        parameters.setParseFailedReason(Parameters.FailReason.IO_ERROR);
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

      try {
        List<FileItem> items = upload.parseRequest(new ServletRequestContext(request));
        int postSize = 0;
        for (FileItem item : items) {
          ApplicationPart part = new ApplicationPart(item, location);
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
                parameters.setParseFailedReason(Parameters.FailReason.POST_TOO_LARGE);
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
        parameters.setParseFailedReason(Parameters.FailReason.INVALID_CONTENT_TYPE);
        throw new ServletException(e);
      } catch (SizeException e) {
        parameters.setParseFailedReason(Parameters.FailReason.POST_TOO_LARGE);
//      checkSwallowInput();
        throw new IllegalStateException(e);
      } catch (IOException e) {
        parameters.setParseFailedReason(Parameters.FailReason.IO_ERROR);
        throw e;
      } catch (IllegalStateException e) {
        // addParameters() will set parseFailedReason
//      checkSwallowInput();
      }
    } finally {
      // This might look odd but is correct. setParseFailedReason() only
      // sets the failure reason if none is currently set. This code could
      // be more efficient but it is written this way to be robust with
      // respect to changes in the remainder of the method.
    }
    return parts;
  }

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
          files.add(part.getName(), new LockssMultipartFile(part, filename));
        } else {
          this.multipartParameterNames.add(part.getName());
        }
      }
      setMultipartFiles(files);
    } catch (Throwable ex) {
      handleParseFailure(ex);
    }
  }

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

  @Override
  protected void initializeMultipart() {
    parseRequest(getRequest());
  }

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

  @Override
  public String getMultipartContentType(String paramOrFileName) {
    try {
      Part part = getPart(paramOrFileName);
      return (part != null ? part.getContentType() : null);
    } catch (Throwable ex) {
      throw new MultipartException("Could not access multipart servlet request", ex);
    }
  }

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
   * Spring MultipartFile adapter, wrapping a Servlet Part object.
   */
  @SuppressWarnings("serial")
  private static class LockssMultipartFile implements MultipartFile, Serializable {

    private final Part part;

    private final String filename;

    public LockssMultipartFile(Part part, String filename) {
      this.part = part;
      this.filename = filename;
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

}
