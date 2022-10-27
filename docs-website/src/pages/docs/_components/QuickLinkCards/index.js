import React from "react";
import clsx from "clsx";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";
import styles from "./quicklinkcards.module.scss";

import {
  ThunderboltTwoTone,
  ApiTwoTone,
  DeploymentUnitOutlined,
  SyncOutlined,
  CodeTwoTone,
  QuestionCircleTwoTone,
  SlidersTwoTone,
  HeartTwoTone,
} from "@ant-design/icons";

const quickLinkContent = [
  {
    title: "Get Started",
    icon: <ThunderboltTwoTone />,
    description: "Details on how to get DataHub up and running",
    to: "/docs/get-started-with-datahub",
  },
  {
    title: "Ingest Metadata",
    icon: <ApiTwoTone />,
    description: "Details on how to get Metadata loaded into DataHub",
    to: "/docs/metadata-ingestion",
  },
  {
    title: "Enrich Metadata",
    icon: <DeploymentUnitOutlined />,
    description: "Improve the quality and coverage of Metadata",
    to: "docs/wip/enrich-metadata",
  },
  {
    title: "Act on Metadata",
    icon: <SyncOutlined />,
    description: "Step-by-step guides for acting on Metadata Events",
    to: "docs/wip/act-on-metadata",
  },
  {
    title: "Developer Guides",
    icon: <CodeTwoTone />,
    description: "Interact with DataHub programmatically ",
    to: "/docs/cli",
  },
  {
    title: "Feature Guides",
    icon: <QuestionCircleTwoTone />,
    description: "Step-by-step guides for making the most of DataHub",
    to: "/docs/how/search",
  },
  {
    title: "Deployment Guides",
    icon: <SlidersTwoTone />,
    description: "Step-by-step guides for deploying DataHub to production",
    to: "/docs/deploy/aws",
  },
  {
    title: "Join the Community",
    icon: <HeartTwoTone />,
    description: "Collaborate, learn, and grow with us",
    to: "/docs/slack",
  },
];

const Card = ({ icon, title, description, to }) => {
  return (
    <div className="col col--3">
      <Link to={useBaseUrl(to)} className={clsx("card", styles.feature)}>
        {icon}
        <div>
          <strong>{title}</strong>
          <span>{description}</span>
        </div>
      </Link>
    </div>
  );
};

const QuickLinkCards = () =>
  quickLinkContent?.length > 0 ? (
    <div style={{ padding: "2vh 0" }}>
      <div className="container">
        <div className="row row--no-gutters">
          {quickLinkContent.map((props, idx) => (
            <Card key={idx} {...props} />
          ))}
        </div>
      </div>
    </div>
  ) : null;

export default QuickLinkCards;
