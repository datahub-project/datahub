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
package dataquality.utils;

import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.kie.api.builder.ReleaseId;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.io.ResourceFactory;


/**
 * Created by zechen on 4/27/15.
 */
@Deprecated
public class RuleEngine {
  private static final RuleEngine INSTANCE = new RuleEngine();
  private static final String DRL_PATH = "app/dataquality/rules/diff.drl";

  private KieSession session;

  private RuleEngine() {

  }

  public static RuleEngine getInstance() {
    return INSTANCE;
  }

  public KieSession init() {
    session = createSession(DRL_PATH);
    return session;
  }

  public KieSession refresh() {
    dispose();
    return init();
  }

  public KieSession createSession(String... ruleFilePath) {
    KieServices kService = KieServices.Factory.get();
    KieContainer kContainer = createKieContainer(kService, ruleFilePath);
    return kContainer.newKieSession();
  }


  private KieContainer createKieContainer(KieServices ks, String... drlResourcesPaths) {
    KieFileSystem kfs = ks.newKieFileSystem();
    for (String path : drlResourcesPaths) {
      kfs.write(ResourceFactory.newFileResource(path));
    }
    KieBuilder kbuilder = ks.newKieBuilder(kfs);
    kbuilder.buildAll();
    if (kbuilder.getResults().hasMessages(Message.Level.ERROR)) {
      throw new IllegalArgumentException(kbuilder.getResults().toString());
    }
    ReleaseId relId = kbuilder.getKieModule().getReleaseId();
    return ks.newKieContainer(relId);
  }


  public void dispose() {
    if (session != null) {
      session.dispose();
    }
  }

  public KieSession getSession() {
    return session;
  }

  public void setSession(KieSession session) {
    this.session = session;
  }

}
