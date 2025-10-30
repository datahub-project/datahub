import React from "react";
import styles from "./styles.module.css";

// Health icon components matching DataHub's HealthIcon (same as lineage nodes)
const HealthIcon = ({ health, size = 14 }) => {
  const iconStyle = {
    width: `${size}px`,
    height: `${size}px`,
    display: "inline-block",
    marginLeft: "6px",
    verticalAlign: "middle",
  };

  if (health === "Good") {
    return (
      <svg style={iconStyle} viewBox="0 0 24 24" fill="currentColor">
        <path
          d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"
          fill="#52c41a"
        />
      </svg>
    );
  }

  if (health === "Warning" || health === "Critical") {
    const color = health === "Critical" ? "#ff4d4f" : "#faad14";
    return (
      <svg style={iconStyle} viewBox="0 0 24 24" fill="currentColor">
        <path
          d="M1 21h22L12 2 1 21zm12-3h-2v-2h2v2zm0-4h-2v-4h2v4z"
          fill={color}
        />
      </svg>
    );
  }

  return null;
};

// Simplified version of DataHub's DefaultPreviewCard for tutorials
const DataHubEntityCard = ({
  name,
  type = "Dataset",
  platform = "Hive",
  description,
  owners = [],
  tags = [],
  glossaryTerms = [],
  assertions = { passing: 0, failing: 0, total: 0 },
  health = "Good",
  url = "#",
  className = "",
}) => {
  // Use actual DataHub platform logos from the docs website
  const getPlatformLogo = (platformName) => {
    const logoMap = {
      Hive: "/img/logos/platforms/hive.svg",
      Kafka: "/img/logos/platforms/kafka.svg",
      HDFS: "/img/logos/platforms/hadoop.svg",
      Snowflake: "/img/logos/platforms/snowflake.svg",
      BigQuery: "/img/logos/platforms/bigquery.svg",
      Spark: "/img/logos/platforms/spark.svg",
      PostgreSQL: "/img/logos/platforms/postgres.svg",
      Postgres: "/img/logos/platforms/postgres.svg",
      postgres: "/img/logos/platforms/postgres.svg",
      MySQL: "/img/logos/platforms/mysql.svg",
      MongoDB: "/img/logos/platforms/mongodb.svg",
      Elasticsearch: "/img/logos/platforms/elasticsearch.svg",
      Redshift: "/img/logos/platforms/redshift.svg",
      Databricks: "/img/logos/platforms/databricks.png",
      dbt: "/img/logos/platforms/dbt.svg",
      Airflow: "/img/logos/platforms/airflow.svg",
      Looker: "/img/logos/platforms/looker.svg",
      Tableau: "/img/logos/platforms/tableau.png",
      PowerBI: "/img/logos/platforms/powerbi.png",
      Superset: "/img/logos/platforms/superset.svg",
    };
    return logoMap[platformName] || "/img/logos/platforms/acryl.svg";
  };

  const healthColors = {
    Good: "#52c41a",
    Warning: "#faad14",
    Critical: "#ff4d4f",
  };

  // Get ownership type icon based on type
  const getOwnershipTypeIcon = (ownershipType) => {
    switch (ownershipType) {
      case "Technical Owner":
        return "👨‍💻";
      case "Business Owner":
        return "👔";
      case "Data Steward":
        return "🛡️";
      case "Data Owner":
        return "📊";
      default:
        return "👤";
    }
  };

  // Get assertion status icon
  const getAssertionStatusIcon = (assertions) => {
    if (assertions.total === 0) return null;
    if (assertions.failing > 0) return "❌";
    if (assertions.passing === assertions.total) return "✅";
    return "⚠️";
  };

  // Generate color hash for tags (matching DataHub's ColorHash)
  const generateTagColor = (tagName) => {
    let hash = 0;
    for (let i = 0; i < tagName.length; i++) {
      const char = tagName.charCodeAt(i);
      hash = (hash << 5) - hash + char;
      hash = hash & hash;
    }
    const hue = Math.abs(hash) % 360;
    return `hsl(${hue}, 70%, 45%)`;
  };

  // Generate color for glossary terms
  const generateTermColor = (termName) => {
    const colors = [
      "#1890ff",
      "#52c41a",
      "#faad14",
      "#f5222d",
      "#722ed1",
      "#fa541c",
      "#13c2c2",
      "#eb2f96",
      "#a0d911",
      "#fadb14",
    ];
    let hash = 0;
    for (let i = 0; i < termName.length; i++) {
      hash = (hash << 5) - hash + termName.charCodeAt(i);
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

  return (
    <div className={`${styles.entityCard} ${className}`}>
      <div className={styles.header}>
        <div className={styles.platformInfo}>
          <img
            src={getPlatformLogo(platform)}
            alt={`${platform} logo`}
            className={styles.platformLogo}
          />
          <span className={styles.type}>{type}</span>
          <span className={styles.divider}>•</span>
          <span className={styles.platform}>{platform}</span>
        </div>
      </div>

      <div className={styles.content}>
        <h3 className={styles.entityName}>
          <a href={url} className={styles.entityLink}>
            {name}
            {health && <HealthIcon health={health} size={14} />}
          </a>
        </h3>

        {description && <p className={styles.description}>{description}</p>}

        {(tags.length > 0 || glossaryTerms.length > 0) && (
          <div className={styles.tagTermGroup}>
            {tags.map((tag, index) => (
              <DataHubTag key={`tag-${index}`} tag={tag} />
            ))}
            {glossaryTerms.map((term, index) => (
              <DataHubTerm key={`term-${index}`} term={term} />
            ))}
          </div>
        )}

        {owners.length > 0 && (
          <div className={styles.ownership}>
            <div className={styles.ownershipHeader}>Ownership</div>
            {(() => {
              // Group owners by type
              const ownersByType = {};
              owners.forEach((owner) => {
                const type = owner.type || "Technical Owner";
                if (!ownersByType[type]) ownersByType[type] = [];
                ownersByType[type].push(owner);
              });

              return Object.entries(ownersByType).map(([type, typeOwners]) => (
                <div key={type} className={styles.ownershipTypeGroup}>
                  <div className={styles.ownershipTypeHeader}>
                    <span className={styles.ownershipTypeIcon}>
                      {getOwnershipTypeIcon(type)}
                    </span>
                    <span className={styles.ownershipTypeName}>{type}</span>
                  </div>
                  <div className={styles.ownersList}>
                    {typeOwners.map((owner, index) => (
                      <span key={index} className={styles.owner}>
                        {owner.name || owner}
                      </span>
                    ))}
                  </div>
                </div>
              ));
            })()}
          </div>
        )}

        {assertions.total > 0 && (
          <div className={styles.assertions}>
            <span className={styles.assertionsLabel}>Assertions:</span>
            <span className={styles.assertionStatus}>
              {getAssertionStatusIcon(assertions)}
              <span className={styles.assertionText}>
                {assertions.passing}/{assertions.total} passing
              </span>
            </span>
          </div>
        )}
      </div>
    </div>
  );
};

// Pre-configured sample entities for tutorials
export const SampleEntities = {
  userCreatedTable: {
    name: "fct_users_created",
    type: "Table",
    platform: "Hive",
    description:
      "Fact table tracking user creation events with timestamps and attribution",
    owners: [
      { name: "john.doe@company.com", type: "Technical Owner" },
      { name: "sarah.smith@company.com", type: "Business Owner" },
    ],
    tags: ["PII", "User Analytics", "Daily"],
    glossaryTerms: ["User Metrics", "Fact Table"],
    assertions: { passing: 8, failing: 0, total: 8 },
    health: "Good",
  },

  userDeletedTable: {
    name: "fct_users_deleted",
    type: "Table",
    platform: "Hive",
    description: "Fact table tracking user deletion events and reasons",
    owners: [{ name: "john.doe@company.com", type: "Technical Owner" }],
    tags: ["User Analytics", "Daily"],
    glossaryTerms: ["User Metrics"],
    assertions: { passing: 5, failing: 1, total: 6 },
    health: "Good",
  },

  kafkaUserEvents: {
    name: "user_events",
    type: "Topic",
    platform: "Kafka",
    description: "Real-time stream of user activity events",
    owners: [
      { name: "data.engineering@company.com", type: "Technical Owner" },
      { name: "mike.wilson@company.com", type: "Data Steward" },
    ],
    tags: ["Streaming", "Real-time", "PII"],
    glossaryTerms: ["User Activity", "Event Data"],
    assertions: { passing: 12, failing: 0, total: 12 },
    health: "Good",
  },

  rawUserData: {
    name: "raw_user_data",
    type: "Dataset",
    platform: "HDFS",
    description:
      "Raw user registration and profile data from application database",
    owners: [{ name: "data.platform@company.com", type: "Data Owner" }],
    tags: ["Raw", "PII", "Hourly"],
    glossaryTerms: ["Source Data", "User Information"],
    assertions: { passing: 3, failing: 2, total: 5 },
    health: "Warning",
  },
};

export default DataHubEntityCard;
