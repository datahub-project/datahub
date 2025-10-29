import React from 'react';
import styles from './styles.module.css';

// Simplified version of DataHub's LineageEntityNode for tutorials
const DataHubLineageNode = ({
  name,
  type = 'Dataset',
  entityType = 'Dataset', // DataHub entity type (Dataset, DataJob, etc.)
  platform = 'Hive',
  isSelected = false,
  isCenter = false,
  health = 'Good',
  isExpanded = false,
  columns = [],
  tags = [],
  glossaryTerms = [],
  onClick,
  onToggleExpand,
  className = '',
}) => {
  // Use actual DataHub platform logos from the docs website
  const getPlatformLogo = (platformName) => {
    const logoMap = {
      // Analytics & BI
      'Looker': '/img/logos/platforms/looker.svg',
      'Tableau': '/img/logos/platforms/tableau.png',
      'PowerBI': '/img/logos/platforms/powerbi.png',
      'Metabase': '/img/logos/platforms/metabase.svg',
      'Superset': '/img/logos/platforms/superset.svg',
      'Mode': '/img/logos/platforms/mode.png',
      'Preset': '/img/logos/platforms/presetlogo.svg',
      'Sigma': '/img/logos/platforms/sigma.png',
      'Qlik': '/img/logos/platforms/qlik.png',
      'Redash': '/img/logos/platforms/redash.svg',
      
      // Cloud Data Warehouses
      'Snowflake': '/img/logos/platforms/snowflake.svg',
      'BigQuery': '/img/logos/platforms/bigquery.svg',
      'Redshift': '/img/logos/platforms/redshift.svg',
      'Databricks': '/img/logos/platforms/databricks.png',
      'Synapse': '/img/logos/platforms/mssql.svg',
      
      // Databases
      'PostgreSQL': '/img/logos/platforms/postgres.svg',
      'Postgres': '/img/logos/platforms/postgres.svg',
      'postgres': '/img/logos/platforms/postgres.svg',
      'MySQL': '/img/logos/platforms/mysql.svg',
      'Oracle': '/img/logos/platforms/oracle.svg',
      'SQL Server': '/img/logos/platforms/mssql.svg',
      'MongoDB': '/img/logos/platforms/mongodb.svg',
      'Cassandra': '/img/logos/platforms/cassandra.png',
      'Neo4j': '/img/logos/platforms/neo4j.png',
      'DynamoDB': '/img/logos/platforms/dynamodb.png',
      'ClickHouse': '/img/logos/platforms/clickhouse.svg',
      'CockroachDB': '/img/logos/platforms/cockroachdb.png',
      'MariaDB': '/img/logos/platforms/mariadb.png',
      'Teradata': '/img/logos/platforms/teradata.svg',
      'Vertica': '/img/logos/platforms/vertica.svg',
      'SAP HANA': '/img/logos/platforms/hana.svg',
      'Couchbase': '/img/logos/platforms/couchbase.svg',
      
      // Big Data & Processing
      'Hive': '/img/logos/platforms/hive.svg',
      'Spark': '/img/logos/platforms/spark.svg',
      'Hadoop': '/img/logos/platforms/hadoop.svg',
      'Kafka': '/img/logos/platforms/kafka.svg',
      'Pulsar': '/img/logos/platforms/pulsar.png',
      'Presto': '/img/logos/platforms/presto.svg',
      'Trino': '/img/logos/platforms/trino.png',
      'Druid': '/img/logos/platforms/druid.svg',
      'Pinot': '/img/logos/platforms/pinot.svg',
      'Kusto': '/img/logos/platforms/kusto.svg',
      'Iceberg': '/img/logos/platforms/iceberg.png',
      'Delta Lake': '/img/logos/platforms/deltalake.svg',
      'Hudi': '/img/logos/platforms/hudi.png',
      
      // Cloud Storage
      'S3': '/img/logos/platforms/s3.svg',
      'GCS': '/img/logos/platforms/gcs.svg',
      'ADLS': '/img/logos/platforms/adls.svg',
      
      // ETL & Orchestration
      'Airflow': '/img/logos/platforms/airflow.svg',
      'dbt': '/img/logos/platforms/dbt.svg',
      'Fivetran': '/img/logos/platforms/fivetran.png',
      'Dagster': '/img/logos/platforms/dagster.svg',
      'Prefect': '/img/logos/platforms/prefect.svg',
      'Snaplogic': '/img/logos/platforms/snaplogic.svg',
      'Nifi': '/img/logos/platforms/nifi.svg',
      
      // ML & AI
      'MLflow': '/img/logos/platforms/mlflow.svg',
      'SageMaker': '/img/logos/platforms/sagemaker.svg',
      'Vertex AI': '/img/logos/platforms/vertexai.png',
      
      // Cloud Platforms
      'AWS Athena': '/img/logos/platforms/athena.svg',
      'AWS Glue': '/img/logos/platforms/glue.svg',
      'Azure': '/img/logos/platforms/azure-ad.svg',
      'Elasticsearch': '/img/logos/platforms/elasticsearch.svg',
      
      // Data Quality & Governance
      'Great Expectations': '/img/logos/platforms/great-expectations.png',
      'Feast': '/img/logos/platforms/feast.svg',
      'Dremio': '/img/logos/platforms/dremio.png',
      
      // File Formats & Others
      'OpenAPI': '/img/logos/platforms/openapi.png',
      'Salesforce': '/img/logos/platforms/salesforce.png',
      'Okta': '/img/logos/platforms/okta.png',
      'SAC': '/img/logos/platforms/sac.svg',
      'Hex': '/img/logos/platforms/hex.png',
      'SQLAlchemy': '/img/logos/platforms/sqlalchemy.png',
      'Protobuf': '/img/logos/platforms/protobuf.png',
      
      // DataHub & Default
      'DataHub': '/img/logos/platforms/acryl.svg',
      'API': '/img/logos/platforms/acryl.svg', // Generic for API
      'Unknown': '/img/logos/platforms/acryl.svg',
    };
    return logoMap[platformName] || '/img/logos/platforms/acryl.svg';
  };

  const healthColors = {
    'Good': '#52c41a',
    'Warning': '#faad14', 
    'Critical': '#ff4d4f',
  };

  // Health icon components matching DataHub's HealthIcon
  const HealthIcon = ({ health, size = 16 }) => {
    const iconStyle = {
      width: `${size}px`,
      height: `${size}px`,
      display: 'inline-block',
    };

    if (health === 'Good') {
      return (
        <svg style={iconStyle} viewBox="0 0 24 24" fill="currentColor">
          <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z" fill="#52c41a"/>
        </svg>
      );
    }
    
    if (health === 'Warning' || health === 'Critical') {
      return (
        <svg style={iconStyle} viewBox="0 0 24 24" fill="currentColor">
          <path d="M1 21h22L12 2 1 21zm12-3h-2v-2h2v2zm0-4h-2v-4h2v4z" fill={health === 'Critical' ? '#ff4d4f' : '#faad14'}/>
        </svg>
      );
    }

    return null;
  };

  // Column type icons matching DataHub's exact TypeIcon component
  const getColumnTypeIcon = (columnType) => {
    const iconStyle = { 
      width: '16px', 
      height: '16px', 
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      fontSize: '14px',
      fontWeight: 'bold'
    };
    
    switch (columnType?.toLowerCase()) {
      case 'string':
      case 'varchar':
      case 'text':
        // String icon - A with underline (exactly like DataHub)
        return (
          <div style={{...iconStyle, color: '#1890ff'}}>
            <span style={{ textDecoration: 'underline', fontSize: '12px', fontWeight: 'bold' }}>A</span>
          </div>
        );
      case 'int':
      case 'integer':
      case 'bigint':
      case 'number':
        // Number icon - # symbol (exactly like DataHub)
        return (
          <div style={{...iconStyle, color: '#52c41a'}}>
            <span style={{ fontSize: '14px', fontWeight: 'bold' }}>#</span>
          </div>
        );
      case 'date':
      case 'datetime':
      case 'timestamp':
        // Calendar icon (simple calendar symbol)
        return (
          <div style={{...iconStyle, color: '#fa8c16'}}>
            <svg viewBox="0 0 16 16" width="12" height="12" fill="currentColor">
              <path d="M3.5 0a.5.5 0 0 1 .5.5V1h8V.5a.5.5 0 0 1 1 0V1h1a2 2 0 0 1 2 2v11a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2V3a2 2 0 0 1 2-2h1V.5a.5.5 0 0 1 .5-.5zM1 4v10a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1V4H1z"/>
            </svg>
          </div>
        );
      case 'boolean':
      case 'bool':
        // Boolean icon - simple T/F
        return (
          <div style={{...iconStyle, color: '#722ed1', fontSize: '10px'}}>
            T/F
          </div>
        );
      case 'struct':
      case 'object':
        // Struct icon - curly brackets (exactly like DataHub)
        return (
          <div style={{...iconStyle, color: '#eb2f96', fontSize: '12px'}}>
            { }
          </div>
        );
      case 'array':
      case 'list':
        // Array icon - square brackets
        return (
          <div style={{...iconStyle, color: '#13c2c2', fontSize: '12px'}}>
            [ ]
          </div>
        );
      default:
        // Question mark for unknown types
        return (
          <div style={{...iconStyle, color: '#8c8c8c', fontSize: '12px'}}>
            ?
          </div>
        );
    }
  };

  // Generate color hash for tags (matching DataHub's ColorHash)
  const generateTagColor = (tagName) => {
    // Simple hash function to generate consistent colors
    let hash = 0;
    for (let i = 0; i < tagName.length; i++) {
      const char = tagName.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32bit integer
    }
    
    // Convert to HSL with high saturation for vibrant colors
    const hue = Math.abs(hash) % 360;
    return `hsl(${hue}, 70%, 45%)`;
  };

  // Generate color for glossary terms (matching DataHub's glossary colors)
  const generateTermColor = (termName) => {
    const colors = [
      '#1890ff', '#52c41a', '#faad14', '#f5222d', '#722ed1', 
      '#fa541c', '#13c2c2', '#eb2f96', '#a0d911', '#fadb14'
    ];
    let hash = 0;
    for (let i = 0; i < termName.length; i++) {
      hash = ((hash << 5) - hash) + termName.charCodeAt(i);
    }
    return colors[Math.abs(hash) % colors.length];
  };

  // Tag component matching DataHub's StyledTag
  const DataHubTag = ({ tag }) => (
    <div className={styles.tag}>
      <div 
        className={styles.tagColorDot}
        style={{ backgroundColor: generateTagColor(tag) }}
      />
      <span className={styles.tagText}>{tag}</span>
    </div>
  );

  // Glossary term component matching DataHub's Term
  const DataHubTerm = ({ term }) => (
    <div className={styles.term}>
      <div 
        className={styles.termRibbon}
        style={{ backgroundColor: generateTermColor(term) }}
      />
      <span className={styles.termText}>{term}</span>
    </div>
  );

  // Tags and terms group component
  const TagTermGroup = ({ tags, glossaryTerms, maxShow = 3 }) => {
    const allItems = [
      ...tags.map(tag => ({ type: 'tag', value: tag })),
      ...glossaryTerms.map(term => ({ type: 'term', value: term }))
    ];
    
    const visibleItems = allItems.slice(0, maxShow);
    const remainingCount = allItems.length - maxShow;
    
    return (
      <div className={styles.tagTermGroup}>
        {visibleItems.map((item, index) => (
          item.type === 'tag' ? 
            <DataHubTag key={`tag-${index}`} tag={item.value} /> :
            <DataHubTerm key={`term-${index}`} term={item.value} />
        ))}
        {remainingCount > 0 && (
          <div className={styles.moreCount}>+{remainingCount}</div>
        )}
      </div>
    );
  };

  // Determine if this is a transformation node (DataJob, Query, etc.)
  const isTransformationNode = entityType === 'DataJob' || entityType === 'Query' || entityType === 'DataProcessInstance';
  
  const nodeClasses = [
    isTransformationNode ? styles.transformationNode : styles.lineageNode,
    isSelected && styles.selected,
    isCenter && styles.center,
    className
  ].filter(Boolean).join(' ');

  // Render transformation node (circular, smaller)
  if (isTransformationNode) {
    return (
      <div 
        className={nodeClasses}
        onClick={onClick}
        role="button"
        tabIndex={0}
        title={`${name} (${type})`}
      >
        <div className={styles.transformationIcon}>
          <img 
            src={getPlatformLogo(platform)} 
            alt={`${platform} logo`}
            className={styles.transformationLogo}
          />
        </div>
        <div className={styles.transformationHealthIcon}>
          <HealthIcon health={health} size={12} />
        </div>
      </div>
    );
  }

  // Render entity node (rectangular, larger)
  return (
    <div 
      className={nodeClasses}
      onClick={onClick}
      role="button"
      tabIndex={0}
    >
      {/* Main card content - matches DataHub's CardWrapper structure */}
      <div className={styles.cardWrapper}>
        <div className={styles.nodeHeader}>
          <div className={styles.platformInfo}>
            <img 
              src={getPlatformLogo(platform)} 
              alt={`${platform} logo`}
              className={styles.platformLogo}
            />
            <span className={styles.type}>{type}</span>
          </div>
          <div className={styles.headerActions}>
            {columns.length > 0 && (
              <button
                className={styles.expandButton}
                onClick={(e) => {
                  e.stopPropagation();
                  onToggleExpand && onToggleExpand();
                }}
                title={isExpanded ? 'Hide columns' : 'Show columns'}
              >
                {isExpanded ? '−' : '+'}
              </button>
            )}
          </div>
        </div>
        
        <div className={styles.nodeContent}>
          <div className={styles.nameWithHealth}>
            <div className={styles.nodeName} title={name}>{name}</div>
            <div className={styles.healthIcon}>
              <HealthIcon health={health} size={16} />
            </div>
          </div>
          <div className={styles.platform}>{platform}</div>
          {(tags.length > 0 || glossaryTerms.length > 0) && (
            <TagTermGroup tags={tags} glossaryTerms={glossaryTerms} maxShow={2} />
          )}
        </div>
      </div>
      
      {/* Expandable columns section */}
      {isExpanded && columns.length > 0 && (
        <div className={styles.columnsWrapper}>
          <div className={styles.columnsHeader}>
            <span className={styles.columnsTitle}>Columns ({columns.length})</span>
          </div>
          <div className={styles.columnsList}>
            {columns.map((column, index) => (
              <div key={index} className={styles.columnItem} data-column={column.name}>
                {/* Left handle for incoming connections */}
                <div className={styles.columnHandle} data-position="left" />
                <div className={styles.columnIcon}>
                  {getColumnTypeIcon(column.type)}
                </div>
                <div className={styles.columnInfo}>
                  <span className={styles.columnName}>{column.name}</span>
                  <span className={styles.columnType}>{column.type}</span>
                </div>
                {column.hasLineage && (
                  <div className={styles.lineageIndicator} title="Has column-level lineage">
                    →
                  </div>
                )}
                {/* Right handle for outgoing connections */}
                <div className={styles.columnHandle} data-position="right" />
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

// Component for showing lineage connections with interactive expansion and column-level lineage
export const DataHubLineageFlow = ({ nodes, title, className = '', showColumnLineage = false }) => {
  const [expandedNodes, setExpandedNodes] = React.useState(new Set());

  const toggleNodeExpansion = (nodeId) => {
    setExpandedNodes(prev => {
      const newSet = new Set(prev);
      if (newSet.has(nodeId)) {
        newSet.delete(nodeId);
      } else {
        newSet.add(nodeId);
      }
      return newSet;
    });
  };

  // Column lineage mappings - shows which columns connect between nodes
  const getColumnLineage = (sourceNodeIndex, targetNodeIndex) => {
    // Only show column lineage when going from DataJob to Dataset (after transformation)
    if (sourceNodeIndex === 1 && targetNodeIndex === 2) {
      // DataJob -> fct_users_created (this represents the transformation from user_events through the ETL job)
      return [
        { source: 'user_id', target: 'user_id' },
        { source: 'timestamp', target: 'created_date' },
        { source: 'event_type', target: 'signup_source' },
      ];
    }
    return [];
  };

  const allNodesExpanded = nodes.every(node => expandedNodes.has(node.id));
  const shouldShowColumnConnections = false; // Disabled for now

  return (
    <div className={`${styles.lineageFlow} ${className}`}>
      {title && <h4 className={styles.flowTitle}>{title}</h4>}
      <div className={styles.flowContainer} data-node-count={nodes.length}>
        {nodes.map((node, index) => (
          <React.Fragment key={node.id || index}>
            <DataHubLineageNode 
              {...node} 
              isExpanded={expandedNodes.has(node.id)}
              onToggleExpand={() => toggleNodeExpansion(node.id)}
            />
            {index < nodes.length - 1 && (
              <div 
                className={styles.flowConnection}
                style={{
                  alignSelf: 'center', // Always center vertically
                  justifyContent: 'center' // Center the arrow within the connection
                }}
              >
                <div className={styles.flowArrow}>→</div>
              </div>
            )}
          </React.Fragment>
        ))}
      </div>
    </div>
  );
};

// Component for showing column-level lineage connections
const ColumnLineageConnections = ({ sourceNode, targetNode, connections, hasDataJob = false }) => {
  if (!connections.length) return null;

  return (
    <div className={styles.columnConnections}>
      <svg className={styles.connectionSvg} viewBox="0 0 400 250" preserveAspectRatio="none">
        {connections.map((connection, index) => {
          // When hasDataJob is true, the sourceNode is the DataJob and we need to show 
          // connections from the previous dataset through the DataJob to the target
          let sourceY, targetY;
          
          if (hasDataJob) {
            // Source is DataJob, target is Dataset - show transformation output
            targetY = 50 + (targetNode.columns?.findIndex(col => col.name === connection.target) || 0) * 36;
            // For DataJob source, we'll position the connection at the center
            sourceY = 125; // Center of the DataJob
          } else {
            // Normal dataset to dataset connection
            sourceY = 50 + (sourceNode.columns?.findIndex(col => col.name === connection.source) || 0) * 36;
            targetY = 50 + (targetNode.columns?.findIndex(col => col.name === connection.target) || 0) * 36;
          }
          
          return (
            <g key={`${connection.source}-${connection.target}`}>
              {/* Connection line */}
              <path
                d={`M 30 ${sourceY} Q 200 ${(sourceY + targetY) / 2} 370 ${targetY}`}
                stroke="var(--datahub-primary)"
                strokeWidth="3"
                fill="none"
                opacity="0.9"
                markerEnd="url(#arrowhead)"
              />
              {/* Connection points */}
              <circle cx="30" cy={sourceY} r="4" fill="var(--datahub-primary)" opacity="0.9" />
              <circle cx="370" cy={targetY} r="4" fill="var(--datahub-primary)" opacity="0.9" />
              
              {/* Label showing the transformation */}
              <text
                x="200"
                y={Math.min(sourceY, targetY) - 10}
                textAnchor="middle"
                className={styles.connectionLabel}
                fontSize="10"
                fill="var(--datahub-primary)"
              >
                {connection.source} → {connection.target}
              </text>
            </g>
          );
        })}
        {/* Arrow marker definition */}
        <defs>
          <marker
            id="arrowhead"
            markerWidth="12"
            markerHeight="8"
            refX="11"
            refY="4"
            orient="auto"
          >
            <polygon
              points="0 0, 12 4, 0 8"
              fill="var(--datahub-primary)"
              opacity="0.9"
            />
          </marker>
        </defs>
      </svg>
    </div>
  );
};

// Sample column data for datasets
export const SampleColumns = {
  userEvents: [
    { name: 'user_id', type: 'bigint', hasLineage: true },
    { name: 'event_type', type: 'string', hasLineage: false },
    { name: 'timestamp', type: 'timestamp', hasLineage: true },
    { name: 'properties', type: 'struct', hasLineage: false },
  ],
  userCreated: [
    { name: 'user_id', type: 'bigint', hasLineage: true },
    { name: 'created_date', type: 'date', hasLineage: true },
    { name: 'signup_source', type: 'string', hasLineage: true },
    { name: 'user_email', type: 'string', hasLineage: false },
    { name: 'user_name', type: 'string', hasLineage: false },
  ],
  rawUserData: [
    { name: 'id', type: 'bigint', hasLineage: true },
    { name: 'email', type: 'string', hasLineage: true },
    { name: 'name', type: 'string', hasLineage: true },
    { name: 'created_at', type: 'timestamp', hasLineage: true },
    { name: 'metadata', type: 'struct', hasLineage: false },
    { name: 'is_active', type: 'boolean', hasLineage: false },
  ],
};

// Pre-configured sample lineage flows for tutorials
export const SampleLineageFlows = {
  userMetricsFlow: {
    title: 'User Metrics Data Pipeline',
    nodes: [
      {
        id: 'source',
        name: 'user_events_stream',
        type: 'Topic',
        entityType: 'Dataset',
        platform: 'Kafka', 
        health: 'Good',
        columns: SampleColumns.userEvents,
        tags: ['Streaming', 'Real-time'],
        glossaryTerms: ['User Activity', 'Event Data'],
      },
      {
        id: 'etl',
        name: 'user_transformation_job',
        type: 'ETL Job',
        entityType: 'DataJob',
        platform: 'Databricks',
        health: 'Good',
      },
      {
        id: 'target',
        name: 'user_metrics_fact',
        type: 'Table',
        entityType: 'Dataset',
        platform: 'Snowflake',
        health: 'Good',
        isCenter: true,
        columns: SampleColumns.userCreated,
        tags: ['PII', 'User Analytics', 'Daily'],
        glossaryTerms: ['User Metrics', 'Fact Table'],
      },
    ],
  },
  
  troubleshootingFlow: {
    title: 'Data Quality Investigation Pipeline',
    nodes: [
      {
        id: 'source',
        name: 'customer_transactions',
        type: 'Dataset',
        entityType: 'Dataset',
        platform: 'PostgreSQL',
        health: 'Warning',
        columns: SampleColumns.rawUserData,
        tags: ['Raw', 'PII', 'Hourly'],
        glossaryTerms: ['Source Data', 'Customer Information'],
      },
      {
        id: 'ingestion',
        name: 'fivetran_sync_job',
        type: 'Ingestion Job',
        entityType: 'DataJob',
        platform: 'Fivetran',
        health: 'Good',
      },
      {
        id: 'validation',
        name: 'dbt_quality_checks',
        type: 'Validation Job', 
        entityType: 'DataJob',
        platform: 'dbt',
        health: 'Critical',
      },
      {
        id: 'target',
        name: 'validated_transactions',
        type: 'Table',
        entityType: 'Dataset',
        platform: 'BigQuery',
        health: 'Good',
        isSelected: true,
        columns: SampleColumns.userCreated, // Same schema after cleaning
        tags: ['Validated', 'Clean', 'Production'],
        glossaryTerms: ['Processed Data', 'Transaction Data'],
      },
    ],
  },

  qualityMonitoringFlow: {
    title: 'Quality Monitoring Data Pipeline',
    nodes: [
      {
        id: 'source',
        name: 'raw_transactions',
        type: 'Table',
        entityType: 'Dataset',
        platform: 'PostgreSQL',
        health: 'Warning',
        columns: SampleColumns.rawUserData,
        tags: ['Raw', 'Unvalidated'],
        glossaryTerms: ['Raw Data', 'Transaction Source'],
      },
      {
        id: 'quality',
        name: 'quality_validation_job',
        type: 'Quality Job',
        entityType: 'DataJob',
        platform: 'DataHub',
        health: 'Good',
      },
      {
        id: 'target',
        name: 'validated_transactions',
        type: 'Table',
        entityType: 'Dataset',
        platform: 'Snowflake',
        health: 'Good',
        columns: SampleColumns.userCreated,
        tags: ['Validated', 'Quality-Assured', 'Production'],
        glossaryTerms: ['Validated Data', 'Quality Metrics'],
      },
    ],
  },
};


export default DataHubLineageNode;
