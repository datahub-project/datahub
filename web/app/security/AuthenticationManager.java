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
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import play.Logger;
import play.Play;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

public class AuthenticationManager {

    public static String MASTER_LDAPURL_URL_KEY = "authentication.ldap.url";

    public static String MASTER_PRICIPAL_DOMAIN_KEY = "authentication.principal.domain";

    public static String LDAP_CONTEXT_FACTORY_CLASS_KEY = "authentication.ldap.context_factory_class";

    public static void authenticateUser(String userName, String password) throws NamingException {
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
            }
            catch(NamingException e)
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

}