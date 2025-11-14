# DataHub UI Components Demo

This page demonstrates the DataHub-style UI components that can be embedded in tutorials to provide an authentic DataHub experience.

import DataHubEntityCard, { SampleEntities } from '@site/src/components/DataHubEntityCard';
import DataHubLineageNode, { DataHubLineageFlow, SampleLineageFlows } from '@site/src/components/DataHubLineageNode';

## Entity Cards

These cards mimic the actual DataHub search results and entity previews:

### Sample User Analytics Tables

<DataHubEntityCard {...SampleEntities.userCreatedTable} />
<DataHubEntityCard {...SampleEntities.userDeletedTable} />

### Streaming Data Sources

<DataHubEntityCard {...SampleEntities.kafkaUserEvents} />

### Raw Data Storage

<DataHubEntityCard {...SampleEntities.rawUserData} />

## Lineage Flows

These components show data pipeline relationships using actual DataHub styling:

### User Metrics Pipeline (Basic)

<DataHubLineageFlow {...SampleLineageFlows.userMetricsFlow} />

### User Metrics Pipeline (with Column-Level Lineage)

<DataHubLineageFlow {...SampleLineageFlows.userMetricsFlow} showColumnLineage={true} />

### Troubleshooting Flow

<DataHubLineageFlow {...SampleLineageFlows.troubleshootingFlow} />

## Individual Lineage Nodes

### Dataset Nodes (Rectangular) - With Tags & Glossary Terms

<div style={{display: 'flex', gap: '16px', flexWrap: 'wrap', justifyContent: 'center', margin: '20px 0'}}>
  <DataHubLineageNode 
    name="fct_users_created" 
    type="Table" 
    entityType="Dataset"
    platform="Hive" 
    health="Good"
    isCenter={true}
    tags={['PII', 'User Analytics', 'Daily']}
    glossaryTerms={['User Metrics', 'Fact Table']}
    columns={[
      { name: 'user_id', type: 'bigint', hasLineage: true },
      { name: 'created_date', type: 'date', hasLineage: true },
      { name: 'signup_source', type: 'string', hasLineage: true },
      { name: 'user_email', type: 'string', hasLineage: false },
    ]}
  />
  <DataHubLineageNode 
    name="user_events" 
    type="Topic" 
    entityType="Dataset"
    platform="Kafka" 
    health="Warning"
    tags={['Streaming', 'Real-time']}
    glossaryTerms={['User Activity', 'Event Data']}
    columns={[
      { name: 'user_id', type: 'bigint', hasLineage: true },
      { name: 'event_type', type: 'string', hasLineage: false },
      { name: 'timestamp', type: 'timestamp', hasLineage: true },
      { name: 'properties', type: 'struct', hasLineage: false },
    ]}
  />
  <DataHubLineageNode 
    name="customer_profiles" 
    type="Table" 
    entityType="Dataset"
    platform="Snowflake" 
    health="Critical"
    isSelected={true}
    tags={['Customer Data', 'GDPR', 'Sensitive']}
    glossaryTerms={['Customer Profile', 'Personal Data']}
    columns={[
      { name: 'customer_id', type: 'bigint', hasLineage: true },
      { name: 'profile_data', type: 'struct', hasLineage: false },
      { name: 'last_updated', type: 'timestamp', hasLineage: true },
    ]}
  />
</div>

### Data Job Nodes (Circular)

<div style={{display: 'flex', gap: '16px', flexWrap: 'wrap', justifyContent: 'center', margin: '20px 0'}}>
  <DataHubLineageNode 
    name="user_etl_job" 
    type="ETL Job" 
    entityType="DataJob"
    platform="Spark" 
    health="Good"
  />
  <DataHubLineageNode 
    name="data_validation" 
    type="Validation Job" 
    entityType="DataJob"
    platform="Airflow" 
    health="Critical"
    isSelected={true}
  />
  <DataHubLineageNode 
    name="analytics_pipeline" 
    type="Pipeline" 
    entityType="DataJob"
    platform="dbt" 
    health="Warning"
  />
</div>

## Updated Specifications

The components now match DataHub V3 specifications:

### Dataset Nodes (Rectangular)

- **Width**: 320px (matches `LINEAGE_NODE_WIDTH`)
- **Height**: 90px base + expandable columns section
- **Border Radius**: 12px (DataHub V3 styling)
- **Health Icons**: Actual SVG icons (✓ for Good, ⚠ for Warning/Critical)
- **Expandable Columns**: Click + button to show/hide column details
- **Column Types**: Color-coded icons (Aa for strings, 123 for numbers, etc.)
- **Column Lineage**: → indicator shows columns with lineage connections
- **Column-Level Lineage**: Visual connections between related columns across nodes (when all nodes expanded)
- **Tags**: Color-coded dots with tag names (e.g., PII, Daily, Streaming)
- **Glossary Terms**: Colored ribbon indicators with term names (e.g., User Metrics, Fact Table)

### Data Job Nodes (Circular)

- **Size**: 40px × 40px (matches `TRANSFORMATION_NODE_SIZE`)
- **Border Radius**: 8px (slightly rounded for transformation nodes)
- **Health Icons**: Positioned as badges in top-right corner
- **Platform Logos**: 18px icons centered in the node
- **No Expansion**: Data jobs don't have column-level details

### Entity Cards

- **Colors**: Synced with DataHub Alchemy design system
- **Primary**: `#533FD1` (DataHub violet[500])
- **Border**: `#E9EAEE` (DataHub gray[1400])
- **Text**: `#374066` (DataHub gray[600])

## Benefits of Using Actual DataHub Components

1. **Pixel-Perfect Accuracy**: Matches exact DataHub V3 dimensions and styling
2. **Auto-Sync**: Colors and design tokens automatically sync with DataHub updates
3. **Real Platform Logos**: Uses the actual SVG logos from DataHub's platform library
4. **Consistent Experience**: Users see the exact same UI they'll encounter in DataHub
5. **Future-Proof**: Automatically stays in sync as DataHub UI evolves

## Technical Implementation

These components are now precisely calibrated to DataHub's actual specifications:

- **DataHubEntityCard**: Based on `DefaultPreviewCard` with exact color tokens
- **DataHubLineageNode**: Based on `LineageEntityNode` with V3 dimensions (320x90px)
- **Platform Logos**: Uses the same SVG assets as production DataHub UI
- **Design Tokens**: Automatically extracted from `datahub-web-react/src/alchemy-components/theme/`

The styling is automatically synchronized at build time, ensuring tutorial components always match the production DataHub interface.
