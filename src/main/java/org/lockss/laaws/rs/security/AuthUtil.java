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

import static org.lockss.laaws.rs.configuration.LockssRepositoryConfig.NONE_AUTH_TYPE;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.Arrays;
import java.util.Base64;
import org.lockss.account.UserAccount;
import org.lockss.app.LockssDaemon;
import org.lockss.log.L4JLogger;
import org.lockss.util.FileUtil;

/**
 * Authentication and authorization utility code.
 */
public class AuthUtil {
  private static final L4JLogger log = L4JLogger.getLogger();
  private static final String basicAuthType = "basic";

  private static final String invalidAuthenticationType =
      "Invalid Authentication Type (must be BASIC or NONE).";

  private static String configuredUserName = null;
  private static boolean isConfiguredUserNamePopulated = false;
  private static String configuredPassword = null;
  private static boolean isConfiguredPasswordPopulated = false;

  /**
   * Decodes the basic authorization header.
   *
   * @param header A String with the Authorization header.
   * @return a String[] with the user name and the password.
   */
  public static String[] decodeBasicAuthorizationHeader(String header) {
    log.debug2("header = {}", header);

    // Get the header meaningful bytes.
    byte[] decodedBytes =
        Base64.getDecoder().decode(header.replaceFirst("[B|b]asic ", ""));

    // Check whether nothing was decoded.
    if (decodedBytes == null || decodedBytes.length == 0) {
      // Yes: Done.
      return null;
    }

    // No: Extract the individual credential items, the user name and the
    // password.
    String[] result = new String(decodedBytes).split(":", 2);
    log.debug2("result = [{}, ****]", result[0]);
    return result;
  }

  /**
   * Checks whether the user has the role required to fulfill a set of roles.
   * Throws AccessControlException if the check fails.
   * 
   * @param userName           A String with the user name.
   * @param permissibleRoles   A String... with the roles permissible for the
   *                           user to be able to execute an operation.
   */
  public static void checkAuthorization(String userName,
      String... permissibleRoles) {
    log.debug2("userName = {}", userName);
    log.debug2("permissibleRoles = {}", Arrays.toString(permissibleRoles));

    // Get the user account.
    UserAccount userAccount = null;

    try {
      userAccount =
          LockssDaemon.getLockssDaemon().getAccountManager().getUser(userName);
      log.trace("userAccount.getRoleSet() = {}", userAccount.getRoleSet());
    } catch (Exception e) {
      log.error("userName = {}", userName);
      log.error("LockssDaemon.getLockssDaemon().getAccountManager()."
          + "getUser(" + userName + ")", e);
      throw new AccessControlException("Unable to get user '" + userName + "'");
    }

    // An administrator is always authorized.
    if (userAccount.isUserInRole(org.lockss.spring.auth.Roles.ROLE_USER_ADMIN))
    {
      log.debug2("Authorized as administrator.");
      return;
    }

    // Check whether there are no permissible roles.
    if (permissibleRoles == null || permissibleRoles.length == 0) {
      // Yes: Normal users are not authorized.
      String message = "Unauthorized like any non-administrator";
      log.debug2(message);
      throw new AccessControlException(message);
    }

    // Loop though all the permissible roles.
    for (String permissibleRole : permissibleRoles) {
      log.trace("permissibleRole = {}", permissibleRole);

      // If any role is permitted, this user is authorized.
      if (org.lockss.spring.auth.Roles.ROLE_ANY.equals(permissibleRole)) {
	log.debug2("Authorized like everybody else.");
        return;
      }

      // The user is authorized if it has this permissible role.
      if (userAccount.isUserInRole(permissibleRole)) {
	log.debug2("Authorized because user is in role.");
        return;
      }
    }

    // The user is not authorized because it does not have any of the
    // permissible roles.
    String message = "Unauthorized because user '" + userName
        + "'does not have any of the permissible roles";
    log.debug2(message);
    throw new AccessControlException(message);
  }

  /**
   * Provides an indication of whether authentication is required. Throws
   * AccessControlException if the the specified authentication method is not
   * valid.
   * 
   * @param authenticationType A String with the configured authentication type.
   *
   * @return a boolean with <code>true</code> if authentication is required,
   *         <code>false</code> otherwise.
   */
  public static boolean isAuthenticationOn(String authenticationType) {
    log.debug2("authenticationType = {}", authenticationType);

    // Check whether access is allowed to anybody.
    if (NONE_AUTH_TYPE.equalsIgnoreCase(authenticationType)) {
      // Yes.
      log.debug2("Authentication is OFF.");
      return false;
      // No: Check whether the authentication type is not "basic".
    } else if (!basicAuthType.equalsIgnoreCase(authenticationType)) {
      // Yes: Report the problem.
      log.error(invalidAuthenticationType);
      log.error("authenticationType = {}", authenticationType);

      throw new AccessControlException(authenticationType + ": "
          + invalidAuthenticationType);
    }

    // No.
    log.debug2("Authentication is ON.");
    return true;
  }

  /**
   * Provides the authentication user name.
   * 
   * @param userName A String with the user name to be lazy-loaded on the first
   *                 call and ignored afterwards.
   * @return a String with the authentication user name.
   */
  public static synchronized String getConfiguredUserName(String userName) {
    if (!isConfiguredUserNamePopulated) {
      configuredUserName = userName;
      isConfiguredUserNamePopulated = true;
    }

    return configuredUserName;
  }

  /**
   * Provides the authentication password.
   * 
   * @param passwordFilePathName A String with the path name of a file
   *                             containing the password to be lazy-loaded on
   *                             the first call and ignored afterwards.
   * @param fallbackPassword     A String with a fall-back password to be
   *                             lazy-loaded on the first call and ignored
   *                             afterwards.
   * @return a String with the authentication password.
   */
  public static synchronized String getConfiguredPassword(
      String passwordFilePathName, String fallbackPassword) {
    if (!isConfiguredPasswordPopulated) {
      // Check whether there is a password file path name.
      if (passwordFilePathName != null) {
        // Yes: Get the password in the password file.
        try {
          configuredPassword = FileUtil.readPasswdFile(passwordFilePathName);
        } catch (IOException ioe) {
          log.warn("Exception caught getting service password from file "
              + passwordFilePathName, ioe);
          // The password could not be obtained from the password file: Use the
          // fall-back password.
          configuredPassword = fallbackPassword;
        }
      } else {
        // No: Use the fall-back password.
        configuredPassword = fallbackPassword;
      }

      isConfiguredPasswordPopulated = true;
    }

    return configuredPassword;
  }
}
