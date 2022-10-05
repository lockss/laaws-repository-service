/*

Copyright (c) 2019 Board of Trustees of Leland Stanford Jr. University,
all rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */
package org.lockss.laaws.rs.impl;

import org.lockss.log.L4JLogger;
import org.lockss.util.StringUtil;
import org.lockss.util.UrlUtil;

import java.util.List;

/**
 * The continuation token used to paginate through a list of artifacts.
 * 
 * @author Fernando García-Loygorri
 */
public class ArtifactContinuationToken {
  private final static L4JLogger log = L4JLogger.getLogger();
  private static final String separator = ":";

  private String namespace = null;
  private String auid = null;
  private String uri = null;
  private Integer version = null;
  private Integer iteratorHashCode = null;

  /**
   * Constructor from a web request continuation token.
   * 
   * @param webRequestContinuationToken
   *          A String with the web request continuation token.
   * @throws IllegalArgumentException
   *           if the web request continuation token is not syntactically valid.
   */
  public ArtifactContinuationToken(String webRequestContinuationToken)
      throws IllegalArgumentException {
    log.debug2("webRequestContinuationToken = {}", webRequestContinuationToken);

    String message = "Invalid web request continuation token '"
	+ webRequestContinuationToken + "'";

    // Check whether a non-empty web request continuation token has been passed.
    if (webRequestContinuationToken != null
	&& !webRequestContinuationToken.trim().isEmpty()) {
      // Yes: Parse it.
      List<String> tokenItems = null;

      try {
	tokenItems =
	    StringUtil.breakAt(webRequestContinuationToken.trim(), separator);
	log.trace("tokenItems = {}", tokenItems);

	namespace = UrlUtil.decodeUrl(tokenItems.get(0).trim());
	log.trace("namespace = {}", namespace);

	auid = UrlUtil.decodeUrl(tokenItems.get(1).trim());
	log.trace("auid = {}", auid);

	uri = UrlUtil.decodeUrl(tokenItems.get(2).trim());
	log.trace("uri = {}", uri);

	version = Integer.valueOf(tokenItems.get(3).trim());
	log.trace("version = {}", version);

	iteratorHashCode = Integer.valueOf(tokenItems.get(4).trim());
	log.trace("iteratorHashCode = {}", iteratorHashCode);
      } catch (Exception e) {
	log.warn(message, e);
	throw new IllegalArgumentException(message, e);
      }

      // Validate the format of the web request continuation token.
      if (tokenItems.size() != 5) {
	log.warn(message);
	throw new IllegalArgumentException(message);
      }

      validateMembers();
    }
  }

  /**
   * Constructor from members.
   * 
   * @param namespace     A String with the namespace of the last
   *                         artifact transferred.
   * @param auid             A String with the archival unit identifier of the
   *                         last artifact transferred.
   * @param uri              A String with the URI of the last artifact
   *                         transferred.
   * @param version          An Integer with the version of the last artifact
   *                         transferred.
   * @param iteratorHashCode An Integer with the hash code of the iterator used.
   */
  public ArtifactContinuationToken(String namespace, String auid, String uri,
      Integer version, Integer iteratorHashCode) {
    this.namespace = namespace;
    this.auid = auid;
    this.uri = uri;
    this.version = version;
    this.iteratorHashCode = iteratorHashCode;

    validateMembers();
  }

  /**
   * Provides the namespace of the last artifact transferred.
   * 
   * @return a String with the namespace of the last artifact
   *         transferred.
   */
  public String getCollectionId() {
    return namespace;
  }

  /**
   * Provides the archival unit identifier of the last artifact transferred.
   * 
   * @return a String with the archival unit identifier of the last artifact
   *         transferred.
   */
  public String getAuid() {
    return auid;
  }

  /**
   * Provides the URI of the last artifact transferred.
   * 
   * @return a String with the URI of the last artifact transferred.
   */
  public String getUri() {
    return uri;
  }

  /**
   * Provides the version of the last artifact transferred.
   * 
   * @return an Integer with the version of the last artifact transferred.
   */
  public Integer getVersion() {
    return version;
  }

  /**
   * Provides the hash code of the iterator used.
   * 
   * @return an Integer with the hash code of the iterator used.
   */
  public Integer getIteratorHashCode() {
    return iteratorHashCode;
  }

  /**
   * Provides this object in the form of a web response continuation token.
   * 
   * @return a String with this object in the form of a web response
   *         continuation token.
   */
  public String toWebResponseContinuationToken() {
    if (namespace != null && auid != null && uri != null && version != null
	&& iteratorHashCode != null) {
      String encodedToken = UrlUtil.encodeUrl(namespace) + separator
	  + UrlUtil.encodeUrl(auid) + separator + UrlUtil.encodeUrl(uri)
	  + separator + version + separator + iteratorHashCode;
      log.trace("encodedToken = {}", encodedToken);
      return encodedToken;
    }

    String message = "Cannot get web request continuation token from " + this;
    log.warn(message);
    throw new IllegalArgumentException(message);
  }

  @Override
  public String toString() {
    return "[ArtifactContinuationToken namespace=" + namespace
	+ ", auid=" + auid + ", uri=" + uri + ", version=" + version
	+ ", iteratorHashCode=" + iteratorHashCode + "]";
  }

  /**
   * Verifies the validity of the members of this class.
   */
  private void validateMembers() {
    // Validate that all members are null or all non-null. 
    if ((namespace == null && auid == null && uri == null && version == null
	 && iteratorHashCode != null)
	|| (namespace != null && auid != null && uri != null
	    && version != null && iteratorHashCode == null)) {
      String message = "Invalid member combination: namespace = '"
	  + namespace + "', auid = '" + auid + "', uri = '" + uri
	  + "', version = '" + version + "', iteratorHashCode = '"
	  + iteratorHashCode + "'";
      log.warn(message);
      throw new IllegalArgumentException(message);
    }

    // Validate that the namespace is not empty.
    if (namespace != null && namespace.isEmpty()) {
      String message = "Invalid member: namespace = '" + namespace + "'";
      log.warn(message);
      throw new IllegalArgumentException(message);
    }

    // Validate that the auid is not empty.
    if (auid != null && auid.isEmpty()) {
      String message = "Invalid member: auid = '" + auid + "'";
      log.warn(message);
      throw new IllegalArgumentException(message);
    }

    // Validate that the URI is not empty.
    if (uri != null && uri.isEmpty()) {
      String message = "Invalid member: uri = '" + uri + "'";
      log.warn(message);
      throw new IllegalArgumentException(message);
    }

    // Validate that the version is positive.
    if (version != null && version.intValue() <= 0) {
      String message = "Invalid member: version = '" + version + "'";
      log.warn(message);
      throw new IllegalArgumentException(message);
    }

    // Validate that the iterator hash code is positive.
    if (iteratorHashCode != null && iteratorHashCode.intValue() <= 0) {
      String message =
	  "Invalid member: iteratorHashCode = '" + iteratorHashCode + "'";
      log.warn(message);
      throw new IllegalArgumentException(message);
    }
  }
}
