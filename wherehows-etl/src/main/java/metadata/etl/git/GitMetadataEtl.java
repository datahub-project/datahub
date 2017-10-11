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
package metadata.etl.git;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import metadata.etl.EtlJob;
import wherehows.common.Constant;
import wherehows.common.schemas.GitCommitRecord;
import wherehows.common.utils.GitUtil;
import wherehows.common.writers.FileWriter;


/**
 * Created by zechen on 12/7/15.
 */
@Slf4j
public class GitMetadataEtl extends EtlJob {

  public ClassLoader classLoader = getClass().getClassLoader();
  public static final String COMMIT_OUTPUT_FILE = "commit.csv";

  public GitMetadataEtl(long whExecId, Properties prop) {
    super(whExecId, prop);
  }

  public void extract() throws Exception {
    log.info("git extract");
    String gitHost = this.prop.getProperty(Constant.GIT_HOST_KEY);
    String[] projects = (this.prop.getProperty(Constant.GIT_PROJECT_WHITELIST_KEY)).trim().split("\\s*,\\s*");
    String localDir = this.prop.getProperty(Constant.WH_APP_FOLDER_KEY) + "/" + this.prop.getProperty(Constant.JOB_REF_ID_KEY);
    File dir = new File(localDir);
    if (!dir.exists()) {
      if (!dir.mkdirs()) {
        throw new Exception("can not create metadata directory");
      }
    }
    FileWriter fw = new FileWriter(localDir + "/" + COMMIT_OUTPUT_FILE);
    for (String project : projects) {
      Map<String, String> repos = GitUtil.getRepoListFromProject(GitUtil.getHttpsUrl(gitHost, project));
      for (String repo : repos.keySet()) {
        String repoUri = repos.get(repo);
        String repoDir = localDir + "/" + repo;
        GitUtil.clone(repoUri, repoDir);
        List<GitUtil.CommitMetadata> commitMetadataList = GitUtil.getRepoMetadata(repoDir);
        for (GitUtil.CommitMetadata m : commitMetadataList) {
          fw.append(new GitCommitRecord(m, repoUri));
        }
      }
    }
    fw.close();
  }

  @Override
  public void transform()
      throws Exception {
    log.info("git transform");
    InputStream inputStream = classLoader.getResourceAsStream("jython/GitTransform.py");
    interpreter.execfile(inputStream);
    inputStream.close();
  }

  @Override
  public void load()
      throws Exception {
    log.info("git load");
    InputStream inputStream = classLoader.getResourceAsStream("jython/GitLoad.py");
    interpreter.execfile(inputStream);
    inputStream.close();
  }


}
