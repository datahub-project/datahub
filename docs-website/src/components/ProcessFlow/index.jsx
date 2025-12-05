import React from "react";
import styles from "./styles.module.css";

const ProcessFlow = ({
  title,
  steps,
  type = "horizontal", // 'horizontal', 'vertical', 'circular'
  showNumbers = true,
  animated = true,
}) => {
  const renderStep = (step, index) => (
    <div
      key={index}
      className={`${styles.step} ${animated ? styles.animated : ""}`}
    >
      {showNumbers && <div className={styles.stepNumber}>{index + 1}</div>}
      <div className={styles.stepContent}>
        <div className={styles.stepTitle}>{step.title}</div>
        {step.description && (
          <div className={styles.stepDescription}>{step.description}</div>
        )}
        {step.details && (
          <div className={styles.stepDetails}>
            {step.details.map((detail, i) => (
              <div key={i} className={styles.stepDetail}>
                • {detail}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );

  const renderConnector = (index) => (
    <div key={`connector-${index}`} className={styles.connector}>
      {type === "horizontal" ? "→" : "↓"}
    </div>
  );

  // Detect if we might have overflow (4+ steps in horizontal layout)
  const hasOverflow = type === "horizontal" && steps.length >= 4;

  return (
    <div
      className={`${styles.processFlow} ${styles[type]} ${hasOverflow ? styles.hasOverflow : ""}`}
    >
      {title && <div className={styles.flowTitle}>{title}</div>}

      <div className={styles.flowContainer}>
        {steps.map((step, index) => (
          <React.Fragment key={index}>
            {renderStep(step, index)}
            {index < steps.length - 1 && renderConnector(index)}
          </React.Fragment>
        ))}
      </div>
    </div>
  );
};

// Predefined workflow configurations
export const DataHubWorkflows = {
  discoveryProcess: {
    title: "Enterprise Data Discovery Process",
    steps: [
      {
        title: "Requirements Analysis",
        description: "Define business objectives",
        details: [
          "Identify data needs",
          "Set success criteria",
          "Define scope",
        ],
      },
      {
        title: "Strategic Search",
        description: "Apply targeted queries",
        details: ["Use business terms", "Apply filters", "Refine results"],
      },
      {
        title: "Asset Evaluation",
        description: "Assess data quality",
        details: ["Check freshness", "Review schema", "Validate completeness"],
      },
      {
        title: "Access Planning",
        description: "Understand requirements",
        details: [
          "Check permissions",
          "Review documentation",
          "Plan integration",
        ],
      },
    ],
  },

  lineageAnalysis: {
    title: "5-Hop Lineage Analysis Method",
    steps: [
      {
        title: "Start at Target",
        description: "Begin with dataset of interest",
        details: [
          "Open lineage view",
          "Identify current dataset",
          "Note business context",
        ],
      },
      {
        title: "Trace Upstream",
        description: "Follow data backwards",
        details: [
          "Identify transformations",
          "Check data sources",
          "Document dependencies",
        ],
      },
      {
        title: "Analyze Hops",
        description: "Examine each connection",
        details: [
          "Understand business logic",
          "Check quality gates",
          "Note critical points",
        ],
      },
      {
        title: "Impact Assessment",
        description: "Evaluate change effects",
        details: [
          "Identify affected systems",
          "Assess risk levels",
          "Plan mitigation",
        ],
      },
      {
        title: "Validate Understanding",
        description: "Confirm analysis",
        details: [
          "Review with data owners",
          "Test assumptions",
          "Document findings",
        ],
      },
    ],
  },

  ingestionProcess: {
    title: "Metadata Ingestion Workflow",
    steps: [
      {
        title: "Connection",
        description: "Establish secure connections",
        details: [
          "Configure credentials",
          "Test connectivity",
          "Set up authentication",
        ],
      },
      {
        title: "Discovery",
        description: "Scan data structures",
        details: ["Identify schemas", "Map relationships", "Detect patterns"],
      },
      {
        title: "Extraction",
        description: "Pull comprehensive metadata",
        details: ["Schema information", "Statistics", "Lineage data"],
      },
      {
        title: "Transformation",
        description: "Standardize metadata format",
        details: [
          "Apply business rules",
          "Enrich with context",
          "Validate quality",
        ],
      },
      {
        title: "Loading",
        description: "Store in DataHub",
        details: [
          "Update knowledge graph",
          "Index for search",
          "Enable discovery",
        ],
      },
    ],
  },
};

export default ProcessFlow;
