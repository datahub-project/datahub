import React from "react";
import styles from "./styles.module.css";

const ArchitectureDiagram = ({ type = "integration" }) => {
  if (type === "integration") {
    return (
      <div className={styles.architectureDiagram}>
        <div className={styles.diagramTitle}>
          DataHub Integration Architecture
        </div>

        <div className={styles.diagramContainer}>
          {/* Source Systems Layer */}
          <div className={styles.layer}>
            <div className={styles.layerTitle}>Source Systems</div>
            <div className={styles.nodeGroup}>
              <div className={`${styles.node} ${styles.sourceNode}`}>
                <div className={styles.nodeIcon}>🗄️</div>
                <div className={styles.nodeLabel}>Kafka Streams</div>
                <div className={styles.nodeSubtext}>Real-time Events</div>
              </div>
              <div className={`${styles.node} ${styles.sourceNode}`}>
                <div className={styles.nodeIcon}>🏢</div>
                <div className={styles.nodeLabel}>Hive Tables</div>
                <div className={styles.nodeSubtext}>Data Warehouse</div>
              </div>
              <div className={`${styles.node} ${styles.sourceNode}`}>
                <div className={styles.nodeIcon}>📁</div>
                <div className={styles.nodeLabel}>HDFS Files</div>
                <div className={styles.nodeSubtext}>Data Lake</div>
              </div>
            </div>
          </div>

          {/* Arrows */}
          <div className={styles.arrowLayer}>
            <div className={styles.arrow}>→</div>
            <div className={styles.arrow}>→</div>
            <div className={styles.arrow}>→</div>
          </div>

          {/* DataHub Core Layer */}
          <div className={styles.layer}>
            <div className={styles.layerTitle}>DataHub Core</div>
            <div className={styles.nodeGroup}>
              <div className={`${styles.node} ${styles.coreNode}`}>
                <div className={styles.nodeIcon}>🔗</div>
                <div className={styles.nodeLabel}>Metadata API</div>
                <div className={styles.nodeSubtext}>GraphQL & REST</div>
              </div>
              <div className={`${styles.node} ${styles.coreNode}`}>
                <div className={styles.nodeIcon}>🕸️</div>
                <div className={styles.nodeLabel}>Graph Database</div>
                <div className={styles.nodeSubtext}>Relationships</div>
              </div>
              <div className={`${styles.node} ${styles.coreNode}`}>
                <div className={styles.nodeIcon}>🔍</div>
                <div className={styles.nodeLabel}>Search Index</div>
                <div className={styles.nodeSubtext}>Elasticsearch</div>
              </div>
            </div>
          </div>

          {/* Arrows */}
          <div className={styles.arrowLayer}>
            <div className={styles.arrow}>→</div>
            <div className={styles.arrow}>→</div>
            <div className={styles.arrow}>→</div>
          </div>

          {/* User Interface Layer */}
          <div className={styles.layer}>
            <div className={styles.layerTitle}>User Interface</div>
            <div className={styles.nodeGroup}>
              <div className={`${styles.node} ${styles.uiNode}`}>
                <div className={styles.nodeIcon}>🔎</div>
                <div className={styles.nodeLabel}>Search & Browse</div>
                <div className={styles.nodeSubtext}>Data Discovery</div>
              </div>
              <div className={`${styles.node} ${styles.uiNode}`}>
                <div className={styles.nodeIcon}>🌐</div>
                <div className={styles.nodeLabel}>Lineage View</div>
                <div className={styles.nodeSubtext}>Data Flow</div>
              </div>
              <div className={`${styles.node} ${styles.uiNode}`}>
                <div className={styles.nodeIcon}>📊</div>
                <div className={styles.nodeLabel}>Data Profiles</div>
                <div className={styles.nodeSubtext}>Quality Metrics</div>
              </div>
            </div>
          </div>
        </div>

        <div className={styles.diagramFooter}>
          <div className={styles.dataFlow}>
            <span className={styles.flowLabel}>Data Flow:</span>
            <span className={styles.flowStep}>Extract Metadata</span>
            <span className={styles.flowArrow}>→</span>
            <span className={styles.flowStep}>Process & Store</span>
            <span className={styles.flowArrow}>→</span>
            <span className={styles.flowStep}>Search & Discover</span>
          </div>
        </div>
      </div>
    );
  }

  // Add other diagram types as needed
  return null;
};

export default ArchitectureDiagram;
