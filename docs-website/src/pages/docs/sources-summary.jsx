import React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import FilterBar from "./_components/FilterBar";
import FilterCards from "./_components/FilterCards";
import useGlobalData from "@docusaurus/useGlobalData";
import DropDownFilter from "./_components/DropDownFilter";

const metadata = [
  {
    Path: "docs/lineage/airflow",
    Difficulty: "medium",
    PlatformType: "Orchestrator",
    ConnectionType: "push",
    Features: "stateful-ingestion",
  },
  {
    Path: "docs/generated/ingestion/sources/athena",
    Difficulty: "medium",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "column-lineage, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/azure-ad",
    Difficulty: "hard",
    PlatformType: "Identity Provider",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/bigquery",
    Difficulty: "easy",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features:
      "stateful-ingestion, patch-lineage, ui-ingestion, lower-casing, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/business-glossary",
    Difficulty: "medium",
    PlatformType: "Metadata",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/clickhouse",
    Difficulty: "medium",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "stateful-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/csv",
    Difficulty: "medium",
    PlatformType: "Metadata",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/databricks",
    Difficulty: "easy",
    PlatformType: "ETL-and-Processing",
    ConnectionType: "pull",
    Features: "stateful-ingestion, column-lineage, ui-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/dbt",
    Difficulty: "easy",
    PlatformType: "ETL-and-Processing",
    ConnectionType: "pull",
    Features: "stateful-ingestion, patch-lineage,  ui-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/delta-lake",
    Difficulty: "medium",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/demo-data",
    Difficulty: "medium",
    PlatformType: "Metadata",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/druid",
    Difficulty: "medium",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "stateful-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/elasticsearch",
    Difficulty: "hard",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/feast",
    Difficulty: "hard",
    PlatformType: "AIML",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/file",
    Difficulty: "medium",
    PlatformType: "Metadata",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/file-based-lineage",
    Difficulty: "medium",
    PlatformType: "Metadata",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/glue",
    Difficulty: "medium",
    PlatformType: "Metadata",
    ConnectionType: "pull",
    Features: "stateful-ingestion, status-aspect",
  },
  {
    Path: "metadata-ingestion/integration_docs/great-expectations",
    Difficulty: "medium",
    PlatformType: "Observability",
    ConnectionType: "push",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/hive",
    Difficulty: "easy",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "stateful-ingestion, ui-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/iceberg",
    Difficulty: "easy",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "stateful-ingestion,  ui-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/json-schema",
    Difficulty: "medium",
    PlatformType: "Metadata",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/kafka",
    Difficulty: "easy",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "stateful-ingestion,  ui-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/kafka-connect",
    Difficulty: "medium",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "lower-casing",
  },
  {
    Path: "docs/generated/ingestion/sources/ldap",
    Difficulty: "hard",
    PlatformType: "Identity Provider",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/looker",
    Difficulty: "easy",
    PlatformType: "BI-Tool",
    ConnectionType: "pull",
    Features:
      "stateful-ingestion, column-lineage,  ui-ingestion, lower-casing, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/mariadb",
    Difficulty: "easy",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "stateful-ingestion,  ui-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/metabase",
    Difficulty: "medium",
    PlatformType: "BI-Tool",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/mssql",
    Difficulty: "easy",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "stateful-ingestion,  ui-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/mode",
    Difficulty: "hard",
    PlatformType: "BI-Tool",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/mongodb",
    Difficulty: "medium",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/myssql",
    Difficulty: "easy",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "stateful-ingestion,  ui-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/nifi",
    Difficulty: "medium",
    PlatformType: "Orchestrator",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/okta",
    Difficulty: "hard",
    PlatformType: "Identity Provider",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/openapi",
    Difficulty: "medium",
    PlatformType: "Metadata",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/oracle",
    Difficulty: "medium",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "stateful-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/postgres",
    Difficulty: "easy",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "stateful-ingestion,  ui-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/powerbi",
    Difficulty: "easy",
    PlatformType: "BI-Tool",
    ConnectionType: "pull",
    Features: "stateful-ingestion, lower-casing, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/presto",
    Difficulty: "medium",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "stateful-ingestion,  ui-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/presto-on-hive",
    Difficulty: "easy",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "stateful-ingestion, status-aspect",
  },
  {
    Path: "metadata-integration/java/datahub-protobuf/README",
    Difficulty: "hard",
    PlatformType: "Metadata",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/pulsar",
    Difficulty: "medium",
    PlatformType: "Metadata",
    ConnectionType: "pull",
    Features: "stateful-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/redash",
    Difficulty: "medium",
    PlatformType: "BI-Tool",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/redshift",
    Difficulty: "easy",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "stateful-ingestion,  ui-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/s3",
    Difficulty: "medium",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/sagemaker",
    Difficulty: "medium",
    PlatformType: "AIML",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/salesforce",
    Difficulty: "medium",
    PlatformType: "BI-Tool",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/hana",
    Difficulty: "medium",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "stateful-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/snowflake",
    Difficulty: "easy",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features:
      "stateful-ingestion, column-lineage, patch-lineage,  ui-ingestion, lower-casing, status-aspect",
  },
  {
    Path: "metadata-integration/java/spark-lineage/README",
    Difficulty: "medium",
    PlatformType: "Datastore",
    ConnectionType: "push",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/sqlalchemy",
    Difficulty: "medium",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "stateful-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/superset",
    Difficulty: "medium",
    PlatformType: "BI-Tool",
    ConnectionType: "pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/tableau",
    Difficulty: "easy",
    PlatformType: "BI-Tool",
    ConnectionType: "pull",
    Features: "stateful-ingestion,  ui-ingestion, lower-casing, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/trino",
    Difficulty: "easy",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "stateful-ingestion,  ui-ingestion, status-aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/vertica",
    Difficulty: "medium",
    PlatformType: "Datastore",
    ConnectionType: "pull",
    Features: "stateful-ingestion, status-aspect",
  },
];
function Docs(ingestionSourceContent, siteConfig) {
  const [textState, setTextState] = React.useState("");
  const [filterState, setFilterState] = React.useState([]);

  const filterOptions = {
    Difficulty: new Set(),
    PlatformType: new Set(),
    ConnectionType: new Set(),
    Features: new Set(),
  };

  metadata.forEach((data) => {
    Object.keys(data).map((key) => {
      if (key === "Features") {
        data[key].split(",").forEach((feature) => {
          if (feature === " " || feature === "") return;
          filterOptions[key].add(feature.trim());
        });
      } else if (key !== "Path") filterOptions[key].add(data[key]);
    });
  });

  const filteredIngestionSourceContent = ingestionSourceContent.filter(
    (item) => {
      if (textState === "" && filterState.length === 0) return true;
      else if (filterState.length > 0) {
        let flag = false;
        filterState.forEach((filter) => {
          if (item.tags.includes(filter)) flag = true;
        });
        return flag;
      }
      return item.title.includes(textState);
    }
  );

  return (
    <Layout
      title={siteConfig.tagline}
      description="DataHub is a data discovery application built on an extensible metadata platform that helps you tame the complexity of diverse data ecosystems."
    >
      <header className={"hero"}>
        <div className="container">
          <div className="hero__content">
            <div>
              <h1 className="hero__title">DataHub Integrations</h1>
              <p className="hero__subtitle">
                Services that integrate with DataHub
              </p>
              <FilterBar textState={textState} setTextState={setTextState} />
              <div style={{ display: "flex", justifyContent: "center" }}>
                <DropDownFilter
                  filterState={filterState}
                  setFilterState={setFilterState}
                  filterOptions={filterOptions}
                />
              </div>
            </div>
          </div>
          <FilterCards
            content={filteredIngestionSourceContent}
            filterBar={<FilterBar />}
          />
        </div>
      </header>
    </Layout>
  );
}
function getTags(path) {
  if (path === undefined || path === null) return;
  path = "docs/generated/ingestion/sources/" + path;
  const data = metadata.find((data) => {
    if (data.Path === path) {
      return data;
    }
  });
  let tags = [];
  if (data === undefined) return tags;
  Object.keys(data).map((key) => {
    if (key === "Features") {
      data[key].split(",").forEach((feature) => {
        if (feature === " " || feature === "") return;
        tags.push(feature.trim());
      });
    } else if (key !== "Path") tags.push(data[key]);
  });

  return tags;
}
function DataProviderComponent() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;
  const globalData = useGlobalData();
  const myPluginData =
    globalData["docusaurus-plugin-content-docs"]["default"].versions[0].docs;
  var sourcesList = myPluginData.map((doc) => {
    if (doc.path.includes("/docs/generated/ingestion/sources/")) {
      var tempPath = doc.path;
      tempPath = tempPath.replace("/docs/generated/ingestion/sources/", "");
      tempPath = tempPath.replace(".md", "");
      return tempPath;
    } else return;
  });

  sourcesList = sourcesList.filter(function (element) {
    return element !== undefined;
  });
  const ingestionSourceContent = sourcesList.map((source) => {
    return {
      title: source,
      platformIcon: source,
      description: source,
      tags: getTags(source),
      to: "docs/generated/ingestion/sources/" + source,
    };
  });
  return Docs(ingestionSourceContent, siteConfig);
}

export default DataProviderComponent;
