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
import org.springframework.dao.EmptyResultDataAccessException;
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

    public static String MASTER_LDAPURL_URL_KEY = "authentication.ldap.url";

    public static String MASTER_PRICIPAL_DOMAIN_KEY = "authentication.principal.domain";

    public static String LDAP_CONTEXT_FACTORY_CLASS_KEY = "authentication.ldap.context_factory_class";

    public static String LDAP_DISPLAYNAME_KEY = "displayName";
    public static String LDAP_MAIL_KEY = "mail";
    public static String LDAP_DEPARTMENT_NUMBER_KEY = "departmentNumber";

    public static void authenticateUser(String userName, String password) throws NamingException, SQLException {
        if (userName == null || userName.isEmpty() || password == null || password.isEmpty()) {
            throw new IllegalArgumentException("Username and password can not be blank.");
        }

        if (UserDAO.authenticate(userName, password))
        {
            return;
        }
        else
        {
            DirContext ctx = null;
            try
            {
                Hashtable<String, String> env = buildEnvContext(userName, password);
                ctx = new InitialDirContext(env);
                User user = getAttributes(userName);
                UserDAO.addLdapUserIfNotExist(user);
            }
            catch(NamingException e)
            {
                Logger.error("Ldap authentication failed for user: " + userName);
                Logger.error(e.getMessage());
                throw(e);
            }
            catch(SQLException e)
            {
                Logger.error("Ldap authentication failed for user: " + userName);
                Logger.error(e.getMessage());
                throw(e);
            }
            finally {
                if (ctx != null)
                    ctx.close();
            }
        }

    }

    private static Hashtable<String, String> buildEnvContext(String username,
                                                             String password) {
        Hashtable<String, String> env = new Hashtable<String, String>(11);

        env.put(Context.INITIAL_CONTEXT_FACTORY, Play.application().configuration().getString(LDAP_CONTEXT_FACTORY_CLASS_KEY));
        env.put(Context.PROVIDER_URL, Play.application().configuration().getString(MASTER_LDAPURL_URL_KEY));
        env.put(Context.SECURITY_PRINCIPAL,
                username + Play.application().configuration().getString(MASTER_PRICIPAL_DOMAIN_KEY));
        env.put(Context.SECURITY_CREDENTIALS, password);
        return env;
    }

    public static Map<String, String> getUserAttributes(String userName,
                                                        String... attributeNames) throws NamingException
    {
        if (StringUtils.isBlank(userName))
        {
            throw new IllegalArgumentException("Username and password can not be blank.");
        }

        if (attributeNames.length == 0)
        {
            return Collections.emptyMap();
        }

        Hashtable<String, String> env = buildEnvContext("elabldap", "2authISg00d");

        DirContext ctx = new InitialDirContext(env);

        Attributes matchAttr = new BasicAttributes(true);
        BasicAttribute basicAttr =
                new BasicAttribute("userPrincipalName", userName + "@linkedin.biz");
        matchAttr.put(basicAttr);

        // Attributes to be returned from search
        NamingEnumeration<? extends SearchResult> searchResult =
                ctx.search("ou=Staff Users,dc=linkedin,dc=biz", matchAttr, attributeNames);

        if (ctx != null)
        {
            ctx.close();
        }

        Map<String, String> result = new HashMap<String, String>();

        if (searchResult.hasMore())
        {
            NamingEnumeration<? extends Attribute> attributes =
                    searchResult.next().getAttributes().getAll();

            while (attributes.hasMore())
            {
                Attribute attr = attributes.next();
                String attrId = attr.getID();
                String attrValue = (String) attr.get();
                Logger.error(attrId);

                result.put(attrId, attrValue);
            }
        }

        return result;
    }

    public static User getAttributes(String userName) throws NamingException, SQLException
    {

        Map<String, String> userDetailMap = getUserAttributes(userName,
                LDAP_DISPLAYNAME_KEY,
                LDAP_MAIL_KEY,
                LDAP_DEPARTMENT_NUMBER_KEY);

        String displayName = userDetailMap.get(LDAP_DISPLAYNAME_KEY);
        displayName = displayName.trim().replaceAll(" +", " ");
        String displaNameTokens[] = displayName.split(" ");
        String firstName = displaNameTokens[0];
        String lastName = displaNameTokens[1];
        String email = userDetailMap.get(LDAP_MAIL_KEY);
        String department = userDetailMap.get(LDAP_DEPARTMENT_NUMBER_KEY);
        int departmentNum = 0;
        if (StringUtils.isNotBlank(department))
        {
            try
            {
                departmentNum = Integer.parseInt(department);
            }
            catch(NumberFormatException e)
            {
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