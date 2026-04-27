## Overview

Slack is a documentation or collaboration platform. Learn more in the [official Slack documentation](https://slack.com/).

The DataHub integration for Slack covers document/workspace entities and hierarchy context for knowledge assets. It also captures stateful deletion detection.

## Concept Mapping

| Source Concept                  | DataHub Concept                                       | Notes                                                         |
| ------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------- |
| Workspace scope                 | Platform Instance / Container                         | Organizes Slack metadata context.                             |
| Channel / conversation metadata | Dataset or Document-style entities (module dependent) | Represented based on connector modeling choices.              |
| Users and memberships           | CorpUser / CorpGroup style metadata                   | Used for ownership and collaboration context where supported. |
