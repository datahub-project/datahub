import React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import SearchBar from "./_components/SearchBar";
import QuickLinkCards from "./_components/QuickLinkCards";
import FeatureCardSection from "./_components/FeatureCardSection";
import FeatureCard from "./_components/FeatureCard";

import {
  FolderTwoTone,
  BookTwoTone,
  TagsTwoTone,
  ApiTwoTone,
  EyeTwoTone,
  SearchOutlined,
  CompassTwoTone,
  NodeExpandOutlined,
  CheckCircleTwoTone,
  SafetyCertificateTwoTone,
  LockTwoTone,
  SlackOutlined,
  HistoryOutlined,
  InteractionOutlined,
  GlobalOutlined,
  FileTextOutlined,
} from "@ant-design/icons";

// Quick Link Cards
import {
  ThunderboltTwoTone,
  DeploymentUnitOutlined,
  SyncOutlined,
  CodeTwoTone,
  QuestionCircleTwoTone,
  SlidersTwoTone,
  HeartTwoTone,
  AlertTwoTone
} from "@ant-design/icons";

const introductionContent =  [
  {
    title: "What Is DataHub?",
    description: "Discover DataHub and its features.",
    to: "/",
    icon: <EyeTwoTone />
  },
  {
    title: "DataHub Tour",
    description: "Explore DataHub and its features through visual guides.",
    to: "/",
    icon: <CompassTwoTone />
  },
  {
    title: "Acryl DataHub",
    description: "A hosted version of DataHub.",
    to: "docs/managed-datahub/managed-datahub-overview/",
    icon: <AlertTwoTone />
  },
];

const gettingStartedContent =  [
  {
    title: "Quickstart",
    description: "Deploy DataHub locally in minutes.",
    to: "docs/quickstart",
    icon: <EyeTwoTone />
  },
  {
    title: "Demo",
    description: "Try out DataHub with our demo.",
    to: "https://demo.datahubproject.io",
    icon: <CompassTwoTone />
  },
  {
    title: "Sign Up For Acryl DataHub",
    description: "Contact our team to get started with Acryl DataHub.",
    to: "https://acryldata.io/datahub-signup",
    icon: <AlertTwoTone />
  },
];

const productionContent =  [
  {
    title: "Deployment Guide",
    description: "Learn how to deploy DataHub on GCP, AWS, Azure, and more.",
    to: "/",
    icon: <EyeTwoTone />
  },
  {
    title: "Ingestion Guide",
    description: "Set up scheduled ingestion with just a few clicks.",
    to: "/",
    icon: <CompassTwoTone />
  },
  {
    title: "Moving To Production",
    description: "Explore best practices for transitioning DataHub to production, including security and permissions.",
    to: "/",
    icon: <AlertTwoTone />
  },
];

const developContent =  [
  {
    title: "Developer Use Cases",
    description: "Learn how to use DataHub for your development needs.",
    to: "/",
    icon: <EyeTwoTone />
  },
  {
    title: "API Overview",
    description: "Explore the various APIs available in DataHub.",
    to: "/",
    icon: <CompassTwoTone />
  },
  {
    title: "DataHub CLI",
    description: "Discover the DataHub CLI.",
    to: "/",
    icon: <AlertTwoTone />
  },
  {
    title: "DataHub Actions",
    description: "Learn about DataHub Actions and its use cases.",
    to: "/",
    icon: <AlertTwoTone />
  },
];

function Docs() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  return (
    <div className="container" style={{ padding: "2vh 0"}}>
      <h2>Introduction</h2>
      <div className="row row--no-gutters" style={{ padding: "2vh 0"}} >
        {introductionContent.map((props, idx) => (
          <FeatureCard key={idx} {...props} />
        ))}
      </div>
      <h2>Getting Started</h2>
      <div className="row row--no-gutters" style={{ padding: "2vh 0"}} >
        {gettingStartedContent.map((props, idx) => (
          <FeatureCard key={idx} {...props} />
        ))}
      </div>
      <h2>Move To Production</h2>
      <div className="row row--no-gutters" style={{ padding: "2vh 0"}}>
        {productionContent.map((props, idx) => (
          <FeatureCard key={idx} {...props} />
        ))}
      </div>
      <h2>Develop</h2>
      <div className="row row--no-gutters" style={{ padding: "2vh 0"}}>
        {developContent.map((props, idx) => (
          <FeatureCard key={idx} {...props} />
        ))}
      </div>
    </div>
  );
}

export default Docs;
