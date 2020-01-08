> This doc is for older versions (v0.2.1 and before) of WhereHows. Please refer to [this](https://github.com/linkedin/WhereHows/blob/master/wherehows-etl/README.md) for the latest version.

This is an optional feature.

Source file revision history is quite useful for tracking and determining ownership for jobs/datasets. We will fetch the commit information at the file level for specified projects/repositories in Git.

## Extract
Major related files: [GitMetadataEtl.java](https://github.com/linkedin/WhereHows/blob/master/metadata-etl/src/main/java/metadata/etl/git/GitMetadataEtl.java)

The extract process first finds all repositories under a Gitorious project and then clones the project to a local directory. Later, it extracts the commit information, including the repo urn, commit ID, file path, filename, commit time, committer name, committer email, author name, author email, and commit message from the local repository. JGit is used for Git-related operations.

## Transform
Major related files: [GitTransform.py](https://github.com/linkedin/WhereHows/blob/master/metadata-etl/src/main/resources/jython/GitTransform.py)

Read file into staging table.

## Load
Major related files: [GitLoad.py](https://github.com/linkedin/WhereHows/blob/master/metadata-etl/src/main/resources/jython/GitLoad.py)

Load from staging table to final table