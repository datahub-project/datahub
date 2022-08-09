import React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import SearchBar from "./_components/SearchBar";
import QuickLinkCards from "./_components/QuickLinkCards";
import GuideList from "./_components/GuideList";

import {
  FolderTwoTone,
  BookTwoTone,
  TagsTwoTone,
  ApiTwoTone,
  SearchOutlined,
  CompassTwoTone,
  NodeExpandOutlined,
  CheckCircleTwoTone,
  SafetyCertificateTwoTone,
  LockTwoTone,
  SlackOutlined,
  HistoryOutlined,
} from "@ant-design/icons";

const deploymentGuideContent = [
  {
    title: "Managed DataHub",
    platformIcon: "acryl",
    to: "/docs/quickstart",
  },
  {
    title: "Docker",
    platformIcon: "docker",
    to: "/docs/quickstart",
  },
  {
    title: "AWS ECS",
    platformIcon: "amazon-ecs",
    to: "/docs/quickstart",
  },
  {
    title: "AWS EKS",
    platformIcon: "amazon-eks",
    to: "/docs/quickstart",
  },
  {
    title: "GCP",
    platformIcon: "google-cloud",
    to: "/docs/quickstart",
  },
];

const ingestionGuideContent = [
  {
    title: "Snowflake",
    platformIcon: "snowflake",
    to: "/docs/quickstart",
  },
  {
    title: "Looker",
    platformIcon: "looker",
    to: "/docs/quickstart",
  },
  {
    title: "Redshift",
    platformIcon: "redshift",
    to: "/docs/quickstart",
  },
  {
    title: "Hive",
    platformIcon: "hive",
    to: "/docs/quickstart",
  },
  {
    title: "BigQuery",
    platformIcon: "bigquery",
    to: "/docs/quickstart",
  },
  {
    title: "dbt",
    platformIcon: "dbt",
    to: "/docs/quickstart",
  },
  {
    title: "Athena",
    platformIcon: "athena",
    to: "/docs/quickstart",
  },
  {
    title: "PostgreSQL",
    platformIcon: "postgres",
    to: "/docs/quickstart",
  },
];

const featureGuideContent = [
  { title: "Domains", icon: <FolderTwoTone />, to: "/docs/quickstart" },
  { title: "Glossary Terms", icon: <BookTwoTone />, to: "/docs/quickstart" },
  { title: "Tags", icon: <TagsTwoTone />, to: "/docs/quickstart" },
  { title: "UI-Based Ingestion", icon: <ApiTwoTone />, to: "/docs/quickstart" },
  { title: "Search", icon: <SearchOutlined />, to: "/docs/quickstart" },
  { title: "Browse", icon: <CompassTwoTone />, to: "/docs/quickstart" },
  { title: "Impact Analysis", icon: <NodeExpandOutlined />, to: "/docs/quickstart" },
  { title: "Metadata Tests", icon: <CheckCircleTwoTone />, to: "/docs/quickstart" },
  { title: "Approval Flows", icon: <SafetyCertificateTwoTone />, to: "/docs/quickstart" },
  { title: "Personal Access Tokens", icon: <LockTwoTone />, to: "/docs/quickstart" },
  { title: "Slack Notifications", icon: <SlackOutlined />, to: "/docs/quickstart" },
  { title: "Schema History", icon: <HistoryOutlined />, to: "/docs/quickstart" },
];

function Docs() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  return (
    <Layout
      title={siteConfig.tagline}
      description="DataHub is a data discovery application built on an extensible metadata platform that helps you tame the complexity of diverse data ecosystems."
    >
      <header className={"hero"}>
        <div className="container">
          <div className="hero__content">
            <div>
              <h1 className="hero__title">Documentation</h1>
              <p className="hero__subtitle">Guides and tutorials for everything DataHub.</p>
              <SearchBar />
            </div>
          </div>
          <QuickLinkCards />
          <GuideList title="Deployment Guides" content={deploymentGuideContent} />
          <GuideList title="Ingestion Guides" content={ingestionGuideContent} seeMoreLink={{ label: "See all 36 sources", to: "/docs/quickstart" }} />
          <GuideList title="Feature Guides" content={featureGuideContent} seeMoreLink={{ label: "See all guides", to: "/docs/quickstart" }} />
        </div>
      </header>
    </Layout>
  );
}

export default Docs;
