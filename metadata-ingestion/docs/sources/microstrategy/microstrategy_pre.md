### Overview

This connector extracts metadata from MicroStrategy's REST API, enabling you to catalog and
understand your MicroStrategy BI assets within DataHub.

The plugin extracts **Projects** and **Folders** as containers, **Dashboards (Dossiers)** and
**Reports** as dashboard/chart entities, and **Intelligent Cubes** and **Datasets** as dataset
entities. Optional warehouse lineage connects Intelligent Cubes to their upstream physical tables
via `/api/v2/cubes/{id}/sqlView` SQL output.

### Prerequisites

#### MicroStrategy Environment

- MicroStrategy Library web application with REST API enabled
- MicroStrategy version 10.11 or later (REST API v2 required)
- Network access to MicroStrategy Library from your DataHub environment

#### Authentication

Choose one of the following authentication methods:

**Standard Authentication** — for production environments:

- Service account with metadata read permissions
- Required: Browse permission on target projects, Read access to dashboards/reports/cubes, Use
  permission for REST API

**Anonymous Guest Access** — for demo instances only:

- Only works on MicroStrategy instances configured for guest access (e.g., demo.microstrategy.com)
- Not recommended for production
