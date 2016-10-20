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
import models.User;
import org.apache.commons.lang3.StringUtils;
import play.Logger;
import play.Play;

import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Map;
import java.util.Collections;
import java.util.HashMap;

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

  public static String MASTER_LDAP_URL_KEY = "authentication.ldap.url";
  public static String MASTER_PRINCIPAL_DOMAIN_KEY = "authentication.principal.domain";
  public static String LDAP_CONTEXT_FACTORY_CLASS_KEY = "authentication.ldap.context_factory_class";
  public static String LDAP_SEARCH_BASE_KEY = "authentication.ldap.search.base";

  public static String LDAP_DISPLAY_NAME_KEY = "displayName";
  public static String LDAP_MAIL_KEY = "mail";
  public static String LDAP_DEPARTMENT_NUMBER_KEY = "departmentNumber";

  public static void authenticateUser(String userName, String password)
      throws NamingException, SQLException {
    if (userName == null || userName.isEmpty() || password == null || password.isEmpty()) {
      throw new IllegalArgumentException("Username and password can not be blank.");
    }

    if (UserDAO.authenticate(userName, password)) {
      UserDAO.insertLoginHistory(userName, "default", "SUCCESS", null);
      return;
    }

    final String contextFactories = Play.application().configuration().getString(LDAP_CONTEXT_FACTORY_CLASS_KEY);
    /*  three LDAP properties, each is a '|' separated string of same number of tokens. e.g.
        Url: "ldaps://ldap1.abc.com:1234|ldap://ldap2.abc.com:5678"
        Principal Domain: "@abc.com|@abc.cn"
        Search Base: "ou=Staff Users,dc=abc,dc=com|ou=Staff Users,dc=abc,dc=cn"
     */
    final String[] ldapUrls = Play.application().configuration().getString(MASTER_LDAP_URL_KEY).split("\\s*\\|\\s*");
    final String[] principalDomains =
        Play.application().configuration().getString(MASTER_PRINCIPAL_DOMAIN_KEY).split("\\s*\\|\\s*");
    final String[] ldapSearchBase =
        Play.application().configuration().getString(LDAP_SEARCH_BASE_KEY).split("\\s*\\|\\s*");

    DirContext ctx = null;
    int i;
    for (i = 0; i < ldapUrls.length; i++) {
      try {
        Hashtable<String, String> env =
            buildEnvContext(userName, password, contextFactories, ldapUrls[i], principalDomains[i]);
        ctx = new InitialDirContext(env);
        if (!UserDAO.userExist(userName)) {
          User user = getAttributes(ctx, ldapSearchBase[i], userName, principalDomains[i]);
          UserDAO.addLdapUser(user);
        }
        break;
      } catch (NamingException e) {
        // Logger.error("Ldap authentication failed for user " + userName + " - " + principalDomains[i] + " - " + ldapUrls[i], e);

        // if exhausted all ldap options and can't authenticate user
        if (i >= ldapUrls.length - 1) {
          UserDAO.insertLoginHistory(userName, "LDAP", "FAILURE", e.getMessage());
          throw e;
        }
      } catch (SQLException e) {
        // Logger.error("Ldap authentication SQL error for user: " + userName, e);
        UserDAO.insertLoginHistory(userName, "LDAP", "FAILURE", ldapUrls[i] + e.getMessage());
        throw e;
      } finally {
        if (ctx != null) {
          ctx.close();
        }
      }
    }
    UserDAO.insertLoginHistory(userName, "LDAP", "SUCCESS", ldapUrls[i]);
  }

  private static Hashtable<String, String> buildEnvContext(String username, String password, String contextFactory,
      String ldapUrl, String principalDomain) {
    Hashtable<String, String> env = new Hashtable<>(11);
    env.put(Context.INITIAL_CONTEXT_FACTORY, contextFactory);
    env.put(Context.PROVIDER_URL, ldapUrl);
    env.put(Context.SECURITY_PRINCIPAL, username + principalDomain);
    env.put(Context.SECURITY_CREDENTIALS, password);
    return env;
  }

  public static Map<String, String> getUserAttributes(DirContext ctx, String searchBase, String userName,
      String principalDomain, String... attributeNames)
      throws NamingException {
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

    if (ctx != null) {
      ctx.close();
    }

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

  public static User getAttributes(DirContext ctx, String searchBase, String userName, String principalDomain)
      throws NamingException, SQLException {

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
    user.email = email;
    user.userName = userName;
    user.name = firstName + " " + lastName;
    user.departmentNum = departmentNum;
    return user;
  }
}
