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
package wherehows.common.schemas;

import java.util.ArrayList;
import java.util.List;
import wherehows.common.utils.GitUtil;


/**
 * Created by zechen on 12/8/15.
 */
public class GitCommitRecord extends AbstractRecord {
  String gitRepoUrn;
  String commitId;
  String filePath;
  String fileName;
  Long commitTime;
  String committerName;
  String committerEmail;
  String authorName;
  String authorEmail;
  String message;

  public GitCommitRecord() {
  }

  public GitCommitRecord(GitUtil.CommitMetadata commitMetadata, String gitRepoUrn) {
    this.gitRepoUrn = gitRepoUrn;
    this.commitId = commitMetadata.getCommitId();
    this.filePath = commitMetadata.getFilePath();
    this.fileName = commitMetadata.getFileName();
    this.commitTime = commitMetadata.getCommitTime().getTime() / 1000;
    this.committerName = commitMetadata.getCommitter();
    this.committerEmail = commitMetadata.getCommitterEmail();
    this.authorName = commitMetadata.getAuthor();
    this.authorEmail = commitMetadata.getAuthorEmail();
    this.message = commitMetadata.getMessage();
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(gitRepoUrn);
    allFields.add(commitId);
    allFields.add(filePath);
    allFields.add(fileName);
    allFields.add(commitTime);
    allFields.add(committerName);
    allFields.add(committerEmail);
    allFields.add(authorName);
    allFields.add(authorEmail);
    allFields.add(message);
    return allFields;
  }
}
