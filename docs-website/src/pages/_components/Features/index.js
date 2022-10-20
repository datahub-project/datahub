import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import styles from "./features.module.scss";

import { FolderOpenTwoTone, BuildTwoTone, ApiTwoTone, DeploymentUnitOutlined, NodeCollapseOutlined, HddTwoTone } from "@ant-design/icons";

const featuresContent = [
  {
    title: "Metadata 360",
    icon: <DeploymentUnitOutlined />,
    description: <>Combine technical, operational and business metadata to provide a 360 degree view of your data entities.</>,
  },
  {
    title: "Shift-left",
    icon: <NodeCollapseOutlined />,
    description: (
      <>
        Apply “shift-left” practices to pre-enrich important metadata using ingestion transformers, support for dbt meta-mapping and other features.
      </>
    ),
  },
  {
    title: "Active Metadata",
    icon: <HddTwoTone />,
    description: (
      <>
        Act on changes in metadata in real time by notifying key stakeholders, circuit-breaking business-critcal pipelines, propogating metadata across entites, and more.
      </>
    ),
  },
  {
    title: "Open Source",
    icon: <FolderOpenTwoTone />,
    description: (
      <>
        DataHub was originally <Link to={"https://engineering.linkedin.com/blog/2019/data-hub"}>built at LinkedIn</Link> and subsequently{" "}
        <Link to={"https://github.com/datahub-project/datahub"}>open-sourced</Link> under the Apache 2.0 License. It now has a thriving community with
        over a hundred contributors, and is widely used at many companies.
      </>
    ),
  },
  {
    title: "Forward Looking Architecture",
    icon: <BuildTwoTone />,
    description: (
      <>
        DataHub follows a{" "}
        <Link to={"https://engineering.linkedin.com/blog/2020/datahub-popular-metadata-architectures-explained"}>push-based architecture</Link>, which
        means it's built for continuously changing metadata. The modular design lets it scale with data growth at any organization, from a single
        database under your desk to multiple data centers spanning the globe.
      </>
    ),
  },
  {
    title: "Massive Ecosystem",
    icon: <ApiTwoTone />,
    description: (
      <>
        DataHub has pre-built integrations with your favorite systems: Kafka, Airflow, MySQL, SQL Server, Postgres, LDAP, Snowflake, Hive, BigQuery,
        and <Link to={"docs/metadata-ingestion"}>many others</Link>. The community is continuously adding more integrations, so this list keeps
        getting longer and longer.
      </>
    ),
  },
];

const Feature = ({ icon, title, description }) => {
  return (
    <div className={clsx(styles.feature, "col col--4")}>
      {icon}
      <h3>{title}</h3>
      <p>{description}</p>
    </div>
  );
};

const Features = () =>
  featuresContent?.length > 0 ? (
    <div style={{ padding: "2vh 0" }}>
      <div className="container">
        <div className="row">
          {featuresContent.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </div>
  ) : null;

export default Features;
