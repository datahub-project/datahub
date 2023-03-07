import React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import FilterBar from "./_components/FilterBar";
import FilterCards from "./_components/FilterCards";
import useGlobalData from "@docusaurus/useGlobalData";
import { Row, Col } from "antd";

const metadata = [
  {
    Path: "docs/lineage/airflow",
    logoPath: "img/logos/platforms/airflow.svg",
    Title: "Airflow",
    Description:
      "Airflow is an open-source data orchestration tool used for scheduling, monitoring, and managing complex data pipelines.",
    Difficulty: "Medium",
    "Platform Type": "Orchestrator",
    "Connection Type": "Push",
    Features: "Stateful Ingestion",
  },
  {
    Path: "docs/generated/ingestion/sources/athena",
    logoPath: "img/logos/platforms/athena.svg",
    Title: "Athena",
    Description:
      "Athena is a serverless interactive query service that enables users to analyze data in Amazon S3 using standard SQL.",
    Difficulty: "Medium",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Column Level Lineage, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/azure-ad",
    logoPath: "img/logos/platforms/azure-ad.png",
    Title: "Azure AD",
    Description:
      "Azure AD is a cloud-based identity and access management tool that provides secure authentication and authorization for users and applications.",
    Difficulty: "Hard",
    "Platform Type": "Identity Provider",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/bigquery",
    logoPath: "img/logos/platforms/bigquery.svg",
    Title: "BigQuery",
    Description:
      "BigQuery is a cloud-based data warehousing and analytics tool that allows users to store, query, and analyze large datasets quickly and efficiently.",
    Difficulty: "Easy",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features:
      "Stateful Ingestion, Patch Lineage, UI Ingestion, Lower Casing, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/business-glossary",
    logoPath: "img/datahub-logo-color-mark.svg",
    Title: "Business Glossary",
    Description:
      "Business Glossary is a source provided by DataHub for ingesting glossary metadata that provides a comprehensive list of business terms and definitions used within an organization.",
    Difficulty: "Medium",
    "Platform Type": "Metadata",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/clickhouse",
    logoPath: "img/logos/platforms/clickhouse.svg",
    Title: "ClickHouse",
    Description:
      "ClickHouse is an open-source column-oriented database management system designed for high-performance data processing and analytics.",
    Difficulty: "Medium",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/csv",
    logoPath: "img/datahub-logo-color-mark.svg",
    Title: "CSV",
    Description:
      "An ingestion source for enriching metadata provided in CSV format provided by DataHub",
    Difficulty: "Medium",
    "Platform Type": "Metadata",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/databricks",
    logoPath: "img/logos/platforms/databricks.png",
    Title: "Databricks",
    Description:
      "Databricks is a cloud-based data processing and analytics platform that enables data scientists and engineers to collaborate and build data-driven applications.",
    Difficulty: "Easy",
    "Platform Type": "ETL and Processing",
    "Connection Type": "Pull",
    Features:
      "Stateful Ingestion, Column Level Lineage, UI Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/dbt",
    logoPath: "img/logos/platforms/dbt.svg",
    Title: "dbt",
    Description:
      "dbt is a data transformation tool that enables analysts and engineers to transform data in their warehouses through a modular, SQL-based approach.",
    Difficulty: "Easy",
    "Platform Type": "ETL and Processing",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion, Patch Lineage,  UI Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/delta-lake",
    logoPath: "img/logos/platforms/deltalake.svg",
    Title: "Delta Lake",
    Description:
      "Delta Lake is an open-source data lake storage layer that provides ACID transactions, schema enforcement, and data versioning for big data workloads.",
    Difficulty: "Medium",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/demo-data",
    logoPath: "img/datahub-logo-color-mark.svg",
    Title: "Demo Data",
    Description:
      "Demo Data is a data tool that provides sample data sets for demonstration and testing purposes.",
    Difficulty: "Medium",
    "Platform Type": "Metadata",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/druid",
    logoPath: "img/logos/platforms/druid.svg",
    Title: "Druid",
    Description:
      "Druid is an open-source data store designed for real-time analytics on large datasets.",
    Difficulty: "Medium",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/elasticsearch",
    logoPath: "img/logos/platforms/elasticsearch.svg",
    Title: "Elasticsearch",
    Description:
      "Elasticsearch is a distributed, open-source search and analytics engine designed for handling large volumes of data.",
    Difficulty: "Hard",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/feast",
    logoPath: "img/logos/platforms/feast.svg",
    Title: "Feast",
    Description:
      "Feast is an open-source feature store that enables teams to manage, store, and discover features for machine learning applications.",
    Difficulty: "Hard",
    "Platform Type": "AI+ML",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/file",
    logoPath: "img/datahub-logo-color-mark.svg",
    Title: "File",
    Description: "An ingestion source for single files provided by DataHub",
    Difficulty: "Medium",
    "Platform Type": "Metadata",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/file-based-lineage",
    logoPath: "img/datahub-logo-color-mark.svg",
    Title: "File Based Lineage",
    Description:
      "File Based Lineage is a data tool that tracks the lineage of data files and their dependencies.",
    Difficulty: "Medium",
    "Platform Type": "Metadata",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/glue",
    logoPath: "img/logos/platforms/glue.svg",
    Title: "Glue",
    Description:
      "Glue is a data integration service that allows users to extract, transform, and load data from various sources into a data warehouse.",
    Difficulty: "Medium",
    "Platform Type": "Metadata",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion, Status Aspect",
  },
  {
    Path: "docs/metadata-ingestion/integration_docs/great-expectations",
    logoPath: "img/logos/platforms/great-expectations.png",
    Title: "Great Expectations",
    Description:
      "Great Expectations is an open-source data validation and testing tool that helps data teams maintain data quality and integrity.",
    Difficulty: "Medium",
    "Platform Type": "Observability",
    "Connection Type": "Push",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/hive",
    logoPath: "img/logos/platforms/hive.svg",
    Title: "Hive",
    Description:
      "Hive is a data warehousing tool that facilitates querying and managing large datasets stored in Hadoop Distributed File System (HDFS).",
    Difficulty: "Easy",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion, UI Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/iceberg",
    logoPath: "img/logos/platforms/iceberg.png",
    Title: "Iceberg",
    Description:
      "Iceberg is a data tool that allows users to manage and query large-scale data sets using a distributed architecture.",
    Difficulty: "Easy",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion,  UI Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/json-schema",
    logoPath: "img/datahub-logo-color-mark.svg",
    Title: "JSON Schemas",
    Description:
      "JSON Schemas is a data tool used to define the structure, format, and validation rules for JSON data.",
    Difficulty: "Medium",
    "Platform Type": "Metadata",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/kafka",
    logoPath: "img/logos/platforms/kafka.svg",
    Title: "Kafka",
    Description:
      "Kafka is a distributed streaming platform that allows for the processing and storage of large amounts of data in real-time.",
    Difficulty: "Easy",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion,  UI Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/kafka-connect",
    logoPath: "img/logos/platforms/kafka.svg",
    Title: "Kafka Connect",
    Description:
      "Kafka Connect is an open-source data integration tool that enables the transfer of data between Apache Kafka and other data systems.",
    Difficulty: "Medium",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Lower Casing",
  },
  {
    Path: "docs/generated/ingestion/sources/ldap",
    logoPath: "img/datahub-logo-color-mark.svg",
    Title: "LDAP",
    Description:
      "LDAP (Lightweight Directory Access Protocol) is a data tool used for accessing and managing distributed directory information services over an IP network.",
    Difficulty: "Hard",
    "Platform Type": "Identity Provider",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/looker",
    logoPath: "img/logos/platforms/looker.svg",
    Title: "Looker",
    Description:
      "Looker is a business intelligence and data analytics platform that allows users to explore, analyze, and share data insights in real-time.",
    Difficulty: "Easy",
    "Platform Type": "BI Tool",
    "Connection Type": "Pull",
    Features:
      "Stateful Ingestion, Column Level Lineage,  UI Ingestion, Lower Casing, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/mariadb",
    logoPath: "img/logos/platforms/mariadb.png",
    Title: "MariaDB",
    Description:
      "MariaDB is an open-source relational database management system that is a fork of MySQL.",
    Difficulty: "Easy",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion,  UI Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/metabase",
    logoPath: "img/logos/platforms/metabase.svg",
    Title: "Metabase",
    Description:
      "Metabase is an open-source business intelligence and data visualization tool that allows users to easily query and visualize their data.",
    Difficulty: "Medium",
    "Platform Type": "BI Tool",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/mssql",
    logoPath: "img/logos/platforms/mssql.svg",
    Title: "Microsoft SQL Server",
    Description:
      "Microsoft SQL Server is a relational database management system designed to store, manage, and retrieve data efficiently and securely.",
    Difficulty: "Easy",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion,  UI Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/mode",
    logoPath: "img/logos/platforms/mode.png",
    Title: "Mode",
    Description:
      "Mode is a cloud-based data analysis and visualization platform that enables businesses to explore, analyze, and share data in a collaborative environment.",
    Difficulty: "Hard",
    "Platform Type": "BI Tool",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/mongodb",
    logoPath: "img/logos/platforms/mongodb.svg",
    Title: "MongoDB",
    Description:
      "MongoDB is a NoSQL database that stores data in flexible, JSON-like documents, making it easy to store and retrieve data for modern applications.",
    Difficulty: "Medium",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/mysql",
    logoPath: "img/logos/platforms/mysql.svg",
    Title: "MySQL",
    Description:
      "MySQL is an open-source relational database management system that allows users to store, organize, and retrieve data efficiently.",
    Difficulty: "Easy",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion,  UI Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/nifi",
    logoPath: "img/logos/platforms/nifi.svg",
    Title: "NiFi",
    Description:
      "NiFi is a data integration tool that allows users to automate the flow of data between systems and applications.",
    Difficulty: "Medium",
    "Platform Type": "Orchestrator",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/okta",
    logoPath: "img/logos/platforms/okta.png",
    Title: "Okta",
    Description:
      "Okta is a cloud-based identity and access management tool that enables secure and seamless access to applications and data across multiple devices and platforms.",
    Difficulty: "Hard",
    "Platform Type": "Identity Provider",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/openapi",
    logoPath: "img/logos/platforms/openapi.png",
    Title: "OpenAPI",
    Description:
      "OpenAPI is a specification for building and documenting RESTful APIs.",
    Difficulty: "Medium",
    "Platform Type": "Metadata",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/oracle",
    logoPath: "img/logos/platforms/oracle.svg",
    Title: "Oracle",
    Description:
      "Oracle is a relational database management system that provides a comprehensive and integrated platform for managing and analyzing large amounts of data.",
    Difficulty: "Medium",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/postgres",
    logoPath: "img/logos/platforms/postgres.svg",
    Title: "Postgres",
    Description:
      "Postgres is an open-source relational database management system that provides a powerful tool for storing, managing, and analyzing large amounts of data.",
    Difficulty: "Easy",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion,  UI Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/powerbi",
    logoPath: "img/logos/platforms/powerbi.png",
    Title: "PowerBI",
    Description:
      "PowerBI is a business analytics service by Microsoft that provides interactive visualizations and business intelligence capabilities with an interface simple enough for end users to create their own reports and dashboards.",
    Difficulty: "Easy",
    "Platform Type": "BI Tool",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion, Lower Casing, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/presto",
    logoPath: "img/logos/platforms/presto.svg",
    Title: "Presto",
    Description:
      "Presto is an open-source distributed SQL query engine designed for fast and interactive analytics on large-scale data sets.",
    Difficulty: "Medium",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion,  UI Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/presto-on-hive",
    logoPath: "img/logos/platforms/presto.svg",
    Title: "Presto on Hive",
    Description:
      "Presto on Hive is a data tool that allows users to query and analyze large datasets stored in Hive using SQL-like syntax.",
    Difficulty: "Easy",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion, Status Aspect",
  },
  {
    Path: "docs/metadata-integration/java/datahub-protobuf",
    logoPath: "img/logos/platforms/protobuf.png",
    Title: "Protobuf Schemas",
    Description:
      "Protobuf Schemas is a data tool used for defining and serializing structured data in a compact and efficient manner.",
    Difficulty: "Hard",
    "Platform Type": "Metadata",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/pulsar",
    logoPath: "img/logos/platforms/pulsar.png",
    Title: "Pulsar",
    Description:
      "Pulsar is a real-time data processing and messaging platform that enables high-performance data streaming and processing.",
    Difficulty: "Medium",
    "Platform Type": "Metadata",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/redash",
    logoPath: "img/logos/platforms/redash.svg",
    Title: "Redash",
    Description:
      "Redash is a data visualization and collaboration platform that allows users to connect and query multiple data sources and create interactive dashboards and visualizations.",
    Difficulty: "Medium",
    "Platform Type": "BI Tool",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/redshift",
    logoPath: "img/logos/platforms/redshift.svg",
    Title: "Redshift",
    Description:
      "Redshift is a cloud-based data warehousing tool that allows users to store and analyze large amounts of data in a scalable and cost-effective manner.",
    Difficulty: "Easy",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion,  UI Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/s3",
    logoPath: "img/logos/platforms/s3.svg",
    Title: "S3 Data Lake",
    Description:
      "S3 Data Lake is a cloud-based data storage and management tool that allows users to store, manage, and analyze large amounts of data in a scalable and cost-effective manner.",
    Difficulty: "Medium",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/sagemaker",
    logoPath: "img/logos/platforms/sagemaker.svg",
    Title: "SageMaker",
    Description:
      "SageMaker is a data tool that provides a fully-managed platform for building, training, and deploying machine learning models at scale.",
    Difficulty: "Medium",
    "Platform Type": "AI+ML",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/salesforce",
    logoPath: "img/logos/platforms/salesforce.png",
    Title: "Salesforce",
    Description:
      "Salesforce is a cloud-based customer relationship management (CRM) platform that helps businesses manage their sales, marketing, and customer service activities.",
    Difficulty: "Medium",
    "Platform Type": "BI Tool",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/hana",
    logoPath: "img/logos/platforms/hana.svg",
    Title: "SAP HANA",
    Description:
      "SAP HANA is an in-memory data platform that enables businesses to process large volumes of data in real-time.",
    Difficulty: "Medium",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/snowflake",
    logoPath: "img/logos/platforms/snowflake.svg",
    Title: "Snowflake",
    Description:
      "Snowflake is a cloud-based data warehousing platform that allows users to store, manage, and analyze large amounts of structured and semi-structured data.",
    Difficulty: "Easy",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features:
      "Stateful Ingestion, Column Level Lineage, Patch Lineage,  UI Ingestion, Lower Casing, Status Aspect",
  },
  {
    Path: "docs/metadata-integration/java/spark-lineage",
    logoPath: "img/logos/platforms/spark.svg",
    Title: "Spark",
    Description:
      "Spark is a data processing tool that enables fast and efficient processing of large-scale data sets using distributed computing.",
    Difficulty: "Medium",
    "Platform Type": "Datastore",
    "Connection Type": "Push",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/sqlalchemy",
    logoPath: "img/logos/platforms/sqlalchemy.png",
    Title: "SQLAlchemy",
    Description:
      "SQLAlchemy is a Python-based data tool that provides a set of high-level API for connecting to relational databases and performing SQL operations.",
    Difficulty: "Medium",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/superset",
    logoPath: "img/logos/platforms/superset.svg",
    Title: "Superset",
    Description:
      "Superset is an open-source data exploration and visualization platform that allows users to create interactive dashboards and perform ad-hoc analysis on various data sources.",
    Difficulty: "Medium",
    "Platform Type": "BI Tool",
    "Connection Type": "Pull",
    Features: "",
  },
  {
    Path: "docs/generated/ingestion/sources/tableau",
    logoPath: "img/logos/platforms/tableau.png",
    Title: "Tableau",
    Description:
      "Tableau is a data visualization and business intelligence tool that helps users analyze and present data in a visually appealing and interactive way.",
    Difficulty: "Easy",
    "Platform Type": "BI Tool",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion,  UI Ingestion, Lower Casing, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/trino",
    logoPath: "img/logos/platforms/trino.png",
    Title: "Trino",
    Description:
      "Trino is an open-source distributed SQL query engine designed to query large-scale data processing systems, including Hadoop, Cassandra, and relational databases.",
    Difficulty: "Easy",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion,  UI Ingestion, Status Aspect",
  },
  {
    Path: "docs/generated/ingestion/sources/vertica",
    logoPath: "img/logos/platforms/vertica.png",
    Title: "Vertica",
    Description:
      "Vertica is a high-performance, column-oriented, relational database management system designed for large-scale data warehousing and analytics.",
    Difficulty: "Medium",
    "Platform Type": "Datastore",
    "Connection Type": "Pull",
    Features: "Stateful Ingestion, Status Aspect",
  },
];
function Docs(ingestionSourceContent, siteConfig) {
  const [textState, setTextState] = React.useState("");
  const [filterState, setFilterState] = React.useState([]);

  const filterOptions = {
    Difficulty: new Set(),
    "Platform Type": new Set(),
    "Connection Type": new Set(),
    Features: new Set(),
  };

  metadata.forEach((data) => {
    Object.keys(data).map((key) => {
      if (key === "Features") {
        data[key].split(",").forEach((feature) => {
          if (feature === " " || feature === "") return;
          filterOptions[key].add(feature.trim());
        });
      } else if (
        key !== "Path" &&
        key !== "Title" &&
        key !== "Description" &&
        key !== "logoPath"
      )
        filterOptions[key].add(data[key]);
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
      return (
        item.title.toLowerCase().includes(textState.toLowerCase()) ||
        item.description.toLowerCase().includes(textState.toLowerCase())
      );
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

              <FilterBar
                textState={textState}
                setTextState={setTextState}
                filterState={filterState}
                setFilterState={setFilterState}
                filterOptions={filterOptions}
              />
            </div>
          </div>
        </div>
      </header>

      <FilterCards
        content={filteredIngestionSourceContent}
        filterBar={<FilterBar />}
      />
    </Layout>
  );
}

function getTags(path) {
  if (path === undefined || path === null) return;
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
    } else if (
      key !== "Path" &&
      key !== "Title" &&
      key !== "Description" &&
      key !== "logoPath"
    )
      tags.push(data[key]);
  });
  return tags;
}
function DataProviderComponent() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  const ingestionSourceContent = metadata.map((source) => {
    return {
      title: source.Title,
      platformIcon: source.logoPath,
      description: source.Description,
      tags: getTags(source.Path),
      to: source.Path,
    };
  });

  return Docs(ingestionSourceContent, siteConfig);
}

export default DataProviderComponent;
