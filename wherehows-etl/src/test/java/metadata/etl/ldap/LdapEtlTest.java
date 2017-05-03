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
package metadata.etl.ldap;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Created by zechen on 11/23/15.
 */
public class LdapEtlTest {
  LdapEtl ldapEtl;

  @BeforeMethod
  public void setUp()
      throws Exception {

    ldapEtl = new LdapEtl(300, 0L);
  }

  @Test(groups = {"needConfig"})
  public void testExtract()
      throws Exception {
    ldapEtl.extract();
  }

  @Test(groups = {"needConfig"})
  public void testTransform()
      throws Exception {
    ldapEtl.transform();
  }

  @Test(groups = {"needConfig"})
  public void testLoad()
      throws Exception {
    ldapEtl.load();
  }

  @Test(groups = {"needConfig"})
  public void testRun() throws Exception {
    ldapEtl.run();
  }
}