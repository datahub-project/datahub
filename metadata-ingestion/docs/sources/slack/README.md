## Overview

Slack is a collaboration platform that can be integrated with DataHub to ingest workspace metadata for discovery and governance workflows.

The `slack` module provides ingestion support for Slack entities and related metadata available through Slack APIs.

## Concept Mapping

| Source Concept                  | DataHub Concept                                       | Notes                                                         |
| ------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------- |
| Workspace scope                 | Platform Instance / Container                         | Organizes Slack metadata context.                             |
| Channel / conversation metadata | Dataset or Document-style entities (module dependent) | Represented based on connector modeling choices.              |
| Users and memberships           | CorpUser / CorpGroup style metadata                   | Used for ownership and collaboration context where supported. |
