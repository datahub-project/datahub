/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package security;

import dao.UserDAO;
import wherehows.models.table.User;
import org.apache.commons.lang3.StringUtils;
import play.Logger;
import play.Play;

import java.util.Hashtable;
import java.util.Map;
import java.util.Collections;
import java.util.HashMap;

import javax.naming.AuthenticationException;
import javax.naming.CommunicationException;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchResult;


public class AuthenticationManager {

  private static final String MASTER_LDAP_URL_KEY = "authentication.ldap.url";
  private static final String MASTER_PRINCIPAL_DOMAIN_KEY = "authentication.principal.domain";
  private static final String LDAP_CONTEXT_FACTORY_CLASS_KEY = "authentication.ldap.context_factory_class";
  private static final String LDAP_SEARCH_BASE_KEY = "authentication.ldap.search.base";

  private static final String LDAP_DISPLAY_NAME_KEY = "displayName";
  private static final String LDAP_MAIL_KEY = "mail";
  private static final String LDAP_DEPARTMENT_NUMBER_KEY = "departmentNumber";

  private static final String contextFactories =
      Play.application().configuration().getString(LDAP_CONTEXT_FACTORY_CLASS_KEY);
  /*  three LDAP properties, each is a '|' separated string of same number of tokens. e.g.
      Url: "ldaps://ldap1.abc.com:1234|ldap://ldap2.abc.com:5678"
      Principal Domain: "@abc.com|@abc.cn"
      Search Base: "ou=Staff Users,dc=abc,dc=com|ou=Staff Users,dc=abc,dc=cn"
   */
  private static final String[] ldapUrls =
      Play.application().configuration().getString(MASTER_LDAP_URL_KEY).split("\\s*\\|\\s*");
  private static final String[] principalDomains =
      Play.application().configuration().getString(MASTER_PRINCIPAL_DOMAIN_KEY).split("\\s*\\|\\s*");
  private static final String[] ldapSearchBase =
      Play.application().configuration().getString(LDAP_SEARCH_BASE_KEY).split("\\s*\\|\\s*");


  public static void authenticateUser(String userName, String password) throws NamingException {
    if (userName == null || userName.isEmpty() || password == null || password.isEmpty()) {
      throw new IllegalArgumentException("Username and password can not be blank.");
    }

    // authenticate through WhereHows DB
    if (UserDAO.authenticate(userName, password)) {
      UserDAO.insertLoginHistory(userName, "default", "SUCCESS", null);
      return;
    }

    // authenticate through each LDAP servers
    DirContext ctx = null;
    String message = null;
    boolean authenticated = false;
    for (int i = 0; i < ldapUrls.length; i++) {
      try {
        Hashtable<String, String> env =
            buildEnvContext(userName, password, contextFactories, ldapUrls[i], principalDomains[i]);
        ctx = new InitialDirContext(env);
        if (!UserDAO.userExist(userName)) {
          User user = getAttributes(ctx, ldapSearchBase[i], userName, principalDomains[i]);
          UserDAO.addLdapUser(user);
        }
        authenticated = true;
        message = ldapUrls[i];
        break;
      } catch (CommunicationException e) {
        message = e.toString();
        Logger.error("Ldap server connection error!", e);
      } catch (AuthenticationException e) {
        message = e.toString();
        Logger.trace("Ldap authentication failed for: " + userName + " - " + ldapUrls[i] + " : " + message);
      } catch (NamingException e) {
        message = e.toString();
        Logger.warn("Ldap authentication error for: " + userName + " - " + ldapUrls[i] + " : " + message);
      } finally {
        if (ctx != null) {
          ctx.close();
        }
      }
    }

    UserDAO.insertLoginHistory(userName, "LDAP", authenticated ? "SUCCESS" : "FAILURE", message);
    if (!authenticated) {
      throw new AuthenticationException(message);
    }
  }

  private static Hashtable<String, String> buildEnvContext(String username, String password, String contextFactory,
      String ldapUrl, String principalDomain) {
    Hashtable<String, String> env = new Hashtable<>(11);
    env.put(Context.INITIAL_CONTEXT_FACTORY, contextFactory);
    env.put(Context.PROVIDER_URL, ldapUrl);
    env.put(Context.SECURITY_AUTHENTICATION, "simple");
    env.put(Context.SECURITY_PRINCIPAL, username + principalDomain);
    env.put(Context.SECURITY_CREDENTIALS, password);
    return env;
  }

  private static Map<String, String> getUserAttributes(DirContext ctx, String searchBase, String userName,
      String principalDomain, String... attributeNames) throws NamingException {
    if (StringUtils.isBlank(userName)) {
      throw new IllegalArgumentException("Username and password can not be blank.");
    }

    if (attributeNames.length == 0) {
      return Collections.emptyMap();
    }

    Attributes matchAttr = new BasicAttributes(true);
    BasicAttribute basicAttr = new BasicAttribute("userPrincipalName", userName + principalDomain);
    matchAttr.put(basicAttr);

    NamingEnumeration<? extends SearchResult> searchResult = ctx.search(searchBase, matchAttr, attributeNames);

    Map<String, String> result = new HashMap<>();
    if (searchResult.hasMore()) {
      NamingEnumeration<? extends Attribute> attributes = searchResult.next().getAttributes().getAll();

      while (attributes.hasMore()) {
        Attribute attr = attributes.next();
        String attrId = attr.getID();
        String attrValue = (String) attr.get();

        result.put(attrId, attrValue);
      }
    }
    return result;
  }

  private static User getAttributes(DirContext ctx, String searchBase, String userName, String principalDomain)
      throws NamingException {
    Map<String, String> userDetailMap =
        getUserAttributes(ctx, searchBase, userName, principalDomain, LDAP_DISPLAY_NAME_KEY, LDAP_MAIL_KEY,
            LDAP_DEPARTMENT_NUMBER_KEY);

    String displayName = userDetailMap.get(LDAP_DISPLAY_NAME_KEY);
    String[] displayNameTokens = displayName.trim().replaceAll(" +", " ").split(" ");
    String firstName = displayNameTokens[0];
    String lastName = displayNameTokens[1];
    String email = userDetailMap.get(LDAP_MAIL_KEY);
    String department = userDetailMap.get(LDAP_DEPARTMENT_NUMBER_KEY);
    int departmentNum = 0;
    if (StringUtils.isNotBlank(department)) {
      try {
        departmentNum = Integer.parseInt(department);
      } catch (NumberFormatException e) {
        Logger.error("Convert department number failed. Error message: " + e.getMessage());
        departmentNum = 0;
      }
    }
    User user = new User();
    user.setEmail(email);
    user.setUserName(userName);
    user.setName(firstName + " " + lastName);
    user.setDepartmentNum(departmentNum);
    return user;
  }
}
