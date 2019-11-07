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
package org.lockss.laaws.rs.security;

import static org.lockss.laaws.rs.configuration.LockssRepositoryConfig.PARAM_AUTH_TYPE_KEY;
import static org.lockss.laaws.rs.configuration.LockssRepositoryConfig.DEFAULT_AUTH_TYPE;
import static org.lockss.laaws.rs.configuration.LockssRepositoryConfig.PARAM_USER_NAME_KEY;
import static org.lockss.laaws.rs.configuration.LockssRepositoryConfig.PARAM_USER_PWD_FILE_KEY;
import static org.lockss.laaws.rs.configuration.LockssRepositoryConfig.PARAM_USER_PWD_KEY;
import static org.lockss.laaws.rs.configuration.LockssRepositoryConfig.PARAM_USER_ROLE_KEY;
import static org.lockss.laaws.rs.configuration.LockssRepositoryConfig.DEFAULT_USER_ROLE;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.lockss.log.L4JLogger;
import org.lockss.spring.auth.RequestUriAuthenticationBypass;
import org.lockss.spring.auth.RequestUriAuthenticationBypassImpl;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.GenericFilterBean;

/**
 * Default LOCKSS custom Spring authentication filter.
 */
@Configuration
public class SpringAuthenticationFilter extends GenericFilterBean {

  private static final String noAuthorizationHeader =
      "No authorization header.";
  private static final String noCredentials = "No userid/password credentials.";
  private static final String badCredentials =
      "Bad userid/password credentials.";
  private static final String badConfiguredUser = "Bad configured user.";
  private static final String noUser = "User not found.";
  private final static L4JLogger log = L4JLogger.getLogger();

  /**
   * Authentication filter.
   *
   * @param request  A ServletRequest with the incoming servlet request.
   * @param response A ServletResponse with the outgoing servlet response.
   * @param chain    A FilterChain with the chain where this filter is set.
   * @throws IOException      if there are problems.
   * @throws ServletException if there are problems.
   */
  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain) throws IOException, ServletException {
    log.debug2("Invoked.");

    // Get the type of required authentication.
    String authenticationType =
	getEnvironment().getProperty(PARAM_AUTH_TYPE_KEY, DEFAULT_AUTH_TYPE);
    log.trace("authenticationType = {}", authenticationType);

    HttpServletRequest httpRequest = (HttpServletRequest) request;

    if (log.isTraceEnabled()) {
      StringBuffer originalUrl = httpRequest.getRequestURL();

      if (httpRequest.getQueryString() != null) {
        originalUrl.append("?").append(httpRequest.getQueryString());
      }

      log.trace("originalUrl = {}", originalUrl);
    }

    HttpServletResponse httpResponse = (HttpServletResponse) response;

    try {
      // Check whether authentication is not required at all.
      if (!AuthUtil.isAuthenticationOn(authenticationType)) {
        // Yes: Continue normally.
	log.trace("Authorized (like everybody else).");

        SecurityContextHolder.getContext().setAuthentication(
            getUnauthenticatedUserToken());

        // Continue the chain.
        chain.doFilter(request, response);
        return;
      }
    } catch (AccessControlException ace) {
      // Report the configuration problem.
      String message = ace.getMessage();
      log.error(message);

      SecurityContextHolder.clearContext();
      httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN, message);
      return;
    }

    // No: Check whether this request is available to everybody.
    if (isWorldReachable(httpRequest)) {
      // Yes: Continue normally.
      log.trace("Authenticated (like everybody else).");

      SecurityContextHolder.getContext().setAuthentication(
          getUnauthenticatedUserToken());

      // Continue the chain.
      chain.doFilter(request, response);
      return;
    }

    // No: Get the authorization header.
    String authorizationHeader = httpRequest.getHeader("authorization");
    log.trace("authorizationHeader = {}", authorizationHeader);

    // Check whether no authorization header was found.
    if (authorizationHeader == null) {
      // Yes: Report the problem.
      log.info(noAuthorizationHeader);

      SecurityContextHolder.clearContext();
      httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED,
          noAuthorizationHeader);
      return;
    }

    // No: Get the user credentials in the authorization header.
    String[] credentials =
        AuthUtil.decodeBasicAuthorizationHeader(authorizationHeader);

    // Check whether no credentials were found.
    if (credentials == null) {
      // Yes: Report the problem.
      log.info(noCredentials);

      SecurityContextHolder.clearContext();
      httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED,
          noCredentials);
      return;
    }

    // No: Check whether the found credentials are not what was expected.
    if (credentials.length != 2) {
      // Yes: Report the problem.
      log.info(badCredentials);
      log.info("bad credentials = " + Arrays.toString(credentials));

      SecurityContextHolder.clearContext();
      httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED,
          badCredentials);
      return;
    }

    log.trace("credentials[0] = {}", credentials[0]);

    // No: Get the configured user account information.
    String userName = AuthUtil.getConfiguredUserName(
	getEnvironment().getProperty(PARAM_USER_NAME_KEY));
    log.trace("userName = {}", userName);

    String password = AuthUtil.getConfiguredPassword(
	getEnvironment().getProperty(PARAM_USER_PWD_FILE_KEY),
	getEnvironment().getProperty(PARAM_USER_PWD_KEY));

    // Check whether no configured user was found.
    if (userName == null || password == null) {
      // Yes: Report the problem.
      log.info(badConfiguredUser);

      SecurityContextHolder.clearContext();
      httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED,
	  badConfiguredUser);
      return;
    }

    // Check whether the user name does not match.
    if (!userName.equals(credentials[0])) {
      // Yes: Report the problem.
      log.info(noUser);

      SecurityContextHolder.clearContext();
      httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED,
          badCredentials);
      return;
    }

    // No: Verify the user credentials.
    boolean goodCredentials = password.equals(credentials[1]);
    log.trace("goodCredentials = {}", goodCredentials);

    // Check whether the user credentials are not good.
    if (!goodCredentials) {
      // Yes: Report the problem.
      log.info(badCredentials);
      log.info("userName = {}", userName);
      log.info("bad credentials = {}", Arrays.toString(credentials));

      SecurityContextHolder.clearContext();
      httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED,
          badCredentials);
      return;
    }

    // No: Get the user roles.
    String userRole =
	getEnvironment().getProperty(PARAM_USER_ROLE_KEY, DEFAULT_USER_ROLE);
    log.trace("userRole = {}", userRole);

    Collection<GrantedAuthority> roles = new HashSet<GrantedAuthority>();
    roles.add(new SimpleGrantedAuthority(userRole));
    log.trace("roles = {}", roles);

    // Create the completed authentication details.
    UsernamePasswordAuthenticationToken authentication =
        new UsernamePasswordAuthenticationToken(credentials[0],
            credentials[1], roles);
    log.trace("authentication = {}", authentication);

    // Provide the completed authentication details.
    SecurityContextHolder.getContext().setAuthentication(authentication);
    log.debug("User successfully authenticated");

    // Continue the chain.
    chain.doFilter(request, response);

    log.debug2("Done.");
  }

  /**
   * Provides the completed authentication for an unauthenticated user.
   *
   * @return a UsernamePasswordAuthenticationToken with the completed
   *         authentication details.
   */
  private UsernamePasswordAuthenticationToken getUnauthenticatedUserToken() {
    Collection<GrantedAuthority> roles = new HashSet<GrantedAuthority>();
    roles.add(new SimpleGrantedAuthority("unauthenticatedRole"));

    return new UsernamePasswordAuthenticationToken("unauthenticatedUser",
        "unauthenticatedPassword", roles);
  }

  /**
   * Provides an indication of whether this request is available to everybody.
   *
   * @param httpRequest A HttpServletRequest with the incoming request.
   * @return <code>true</code> if this request is available to everybody,
   *         <code>false</code> otherwise.
   */
  private boolean isWorldReachable(HttpServletRequest httpRequest) {
    log.debug2("Invoked.");

    // Get the HTTP request method name.
    String httpMethodName = httpRequest.getMethod().toUpperCase();
    log.trace("httpMethodName = {}", httpMethodName);

    // Get the HTTP request URI.
    String requestUri = httpRequest.getRequestURI().toLowerCase();
    log.trace("requestUri = {}", requestUri);

    // Determine whether it is world-reachable.
    boolean result = getRequestUriAuthenticationBypass()
        .isWorldReachable(httpMethodName, requestUri);
    log.debug2("result = {}", result);
    return result;
  }

  /**
   * Checks whether the current user has the role required to fulfill a set of
   * roles. Throws AccessControlException if the check fails.
   *
   * @param authenticationType A String with the configured authentication type.
   * @param permissibleRoles   A String... with the roles permissible for the
   *                           user to be able to execute an operation.
   */
  public static void checkAuthorization(String authenticationType,
      String... permissibleRoles) {
    log.debug2("authenticationType = {}", authenticationType);
    log.debug2("permissibleRoles = {}", Arrays.toString(permissibleRoles));

    // Check whether authentication is not required at all.
    if (!AuthUtil.isAuthenticationOn(authenticationType)) {
      // Yes: Continue normally.
      log.debug2("Authorized (like everybody else).");
      return;
    }

    AuthUtil.checkAuthorization(SecurityContextHolder.getContext()
        .getAuthentication().getName(), permissibleRoles);

    log.debug2("Done.");
  }

  /**
   * Provides the authentication bypass.
   *
   * @return a RequestUriAuthenticationBypass with the authentication bypass.
   */
  public RequestUriAuthenticationBypass getRequestUriAuthenticationBypass() {
    return new RequestUriAuthenticationBypassImpl();
  }
}
