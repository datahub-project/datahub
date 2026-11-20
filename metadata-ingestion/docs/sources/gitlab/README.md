## Overview

GitLab hosts source files, documentation, and knowledge bases in git repositories. Learn more in the [official GitLab documentation](https://docs.gitlab.com/).

The DataHub `gitlab-documents` integration ingests markdown and text files from a GitLab project on GitLab.com or self-managed GitLab as Document entities. Folder structure in the project is preserved as parent-child document relationships in DataHub.

## Concept Mapping

| Source Concept       | DataHub Concept | Notes                                                          |
| -------------------- | --------------- | -------------------------------------------------------------- |
| Project folder       | Document        | Optional folder documents for navigation.                      |
| Markdown / text file | Document        | Native (editable) or external (read-only) depending on config. |
| Project + branch     | Custom metadata | Stored on documents for traceability.                          |
