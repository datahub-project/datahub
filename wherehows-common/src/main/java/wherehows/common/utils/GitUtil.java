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
package wherehows.common.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by abhattac on 2/5/15,
 * Modified by zechen on 12/8/15.
 */

public class GitUtil {
  private static final Logger logger = LoggerFactory.getLogger(GitUtil.class);
  private final static String DEFAULT_HOST = "gitli.corp.linkedin.com";
  public static final String HTTPS_PROTOCAL = "https";
  public static final String GIT_PROTOCAL = "git";
  public static final String GIT_SUBFIX = ".git";

  /**
   * Cloning the remote git repo to local directory
   * @param remoteUri remote git url e.g. git://gitli.example.com/project/repo.git
   * @param localDir local destination clone directory
   * @throws IOException
   * @throws GitAPIException
   */
  public static void clone(String remoteUri, String localDir) throws IOException, GitAPIException {
    //create local git directory
    File localGitRepo = new File(localDir);
    if (localGitRepo.exists()) {
      if (localGitRepo.isDirectory()) {
        // clean up directory
        FileUtils.cleanDirectory(localGitRepo);
      } else {
        throw new IOException("File exists: " + localDir);
      }
    } else {
      localGitRepo.mkdirs();
    }

    Git g = Git.cloneRepository().setURI(remoteUri).setDirectory(localGitRepo).call();
    g.close();
  }

  /**
   * Crawlling the project page to get list of repositories, only works for Gitorious
   * @param projectUrl the project url e.g. https://git.example.com/project
   * @return List of path of repositories e.g. project/repo
   * @throws IOException
   */
  public static Map<String, String> getRepoListFromProject(String projectUrl) throws IOException {

    Map<String, String> repoList = new HashMap<>();
    Document doc = Jsoup.connect(projectUrl).data("format", "xml").get();
    Elements repos = doc.getElementsByTag("repositories");
    Elements mainlines = repos.first().getElementsByTag("mainlines");
    Elements repo = mainlines.first().getElementsByTag("repository");

    for (Element e : repo) {
      String repoName = e.getElementsByTag("name").first().text();
      String repoUrl = e.getElementsByTag("clone_url").first().text();
      repoList.put(repoName.trim(), repoUrl.trim());
    }

    return repoList;
  }

  /**
   * Fetch all commit metadata from the repo
   * @param repoDir repository directory
   * @return list of commit metadata
   * @throws IOException
   * @throws GitAPIException
   */
  public static List<CommitMetadata> getRepoMetadata(String repoDir) throws IOException, GitAPIException {

    List<CommitMetadata> metadataList = new ArrayList<>();

    FileRepositoryBuilder builder = new FileRepositoryBuilder();
    Repository repository = builder.setGitDir(new File(repoDir, ".git")).readEnvironment().findGitDir().build();

    // Current branch may not be master. Instead of hard coding determine the current branch
    String currentBranch = repository.getBranch();
    Ref head = repository.getRef("refs/heads/" + currentBranch); // current branch may not be "master"
    if (head == null) {
      return metadataList;
    }

    Git git = new Git(repository);

    RevWalk walk = new RevWalk(repository);
    RevCommit commit = walk.parseCommit(head.getObjectId());

    TreeWalk treeWalk = new TreeWalk(repository);
    treeWalk.addTree(commit.getTree());
    treeWalk.setRecursive(true);
    while (treeWalk.next()) {
      String filePath = treeWalk.getPathString();
      Iterable<RevCommit> commitLog = git.log().add(repository.resolve(Constants.HEAD)).addPath(filePath).call();
      for (RevCommit r : commitLog) {
        CommitMetadata metadata = new CommitMetadata(r.getName());
        metadata.setFilePath(filePath);
        metadata.setFileName(FilenameUtils.getName(filePath));
        metadata.setMessage(r.getShortMessage().trim());
        // Difference between committer and author
        // refer to: http://git-scm.com/book/ch2-3.html
        PersonIdent committer = r.getCommitterIdent();
        PersonIdent author = r.getAuthorIdent();
        metadata.setAuthor(author.getName());
        metadata.setAuthorEmail(author.getEmailAddress());
        metadata.setCommitter(committer.getName());
        metadata.setCommitterEmail(committer.getEmailAddress());
        metadata.setCommitTime(committer.getWhen());
        metadataList.add(metadata);
      }
    }
    git.close();
    return metadataList;
  }

  public static String getHttpsUrl(String host, String path) {
    return HTTPS_PROTOCAL + "://" + host + "/" + path;
  }

  public static String getGitUrl(String host, String path) {
    return GIT_PROTOCAL + "://" + host + "/" + path + GIT_SUBFIX;
  }

  public static String getSshUrl(String host, String path) {
    return GIT_PROTOCAL + "@" + host + ":" + path;
  }

  public static class CommitMetadata {
    String commitId;
    String author;
    String committer;
    Date commitTime;
    String message;
    String committerEmail;
    String authorEmail;
    String filePath;
    String fileName;

    public CommitMetadata() {
    }

    public CommitMetadata(String commitId) {
      this.commitId = commitId;
    }

    public CommitMetadata(String commitId, String author, String committer, Date commitTime, String message,
        String committerEmail, String authorEmail, String filePath, String fileName) {
      this.commitId = commitId;
      this.author = author;
      this.committer = committer;
      this.commitTime = commitTime;
      this.message = message;
      this.committerEmail = committerEmail;
      this.authorEmail = authorEmail;
      this.filePath = filePath;
      this.fileName = fileName;
    }

    public String getCommitId() {
      return commitId;
    }

    public void setCommitId(String commitId) {
      this.commitId = commitId;
    }

    public String getAuthor() {
      return author;
    }

    public void setAuthor(String author) {
      this.author = author;
    }

    public String getCommitter() {
      return committer;
    }

    public void setCommitter(String committer) {
      this.committer = committer;
    }

    public Date getCommitTime() {
      return commitTime;
    }

    public void setCommitTime(Date commitTime) {
      this.commitTime = commitTime;
    }

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }

    public String getCommitterEmail() {
      return committerEmail;
    }

    public void setCommitterEmail(String committerEmail) {
      this.committerEmail = committerEmail;
    }

    public String getAuthorEmail() {
      return authorEmail;
    }

    public void setAuthorEmail(String authorEmail) {
      this.authorEmail = authorEmail;
    }

    public String getFilePath() {
      return filePath;
    }

    public void setFilePath(String filePath) {
      this.filePath = filePath;
    }

    public String getFileName() {
      return fileName;
    }

    public void setFileName(String fileName) {
      this.fileName = fileName;
    }
  }

}

