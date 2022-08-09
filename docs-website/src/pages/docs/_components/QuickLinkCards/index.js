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
    to: "/docs/quickstart",
  },
  {
    title: "Ingest Metadata",
    icon: <ApiTwoTone />,
    description: "Details on how to get Metadata loaded into DataHub",
    to: "/docs/quickstart",
  },
  {
    title: "Enrich Metadata",
    icon: <DeploymentUnitOutlined />,
    description: "Improve the quality and coverage of Metadata",
    to: "/docs/quickstart",
  },
  {
    title: "Act on Metadata",
    icon: <SyncOutlined />,
    description: "Step-by-step guides for acting on Metadata Events",
    to: "/docs/quickstart",
  },
  {
    title: "DataHub API & SDK",
    icon: <CodeTwoTone />,
    description: "Interact with DataHub programmatically ",
    to: "/docs/quickstart",
  },
  {
    title: "Tutorials",
    icon: <QuestionCircleTwoTone />,
    description: "Step-by-step guides for making the most of DataHub",
    to: "/docs/quickstart",
  },
  {
    title: "Advanced Guides",
    icon: <SlidersTwoTone />,
    description: "Step-by-step guides for advanced use-cases and features",
    to: "/docs/quickstart",
  },
  {
    title: "Join the Community",
    icon: <HeartTwoTone />,
    description: "Collaborate, learn, and grow with us",
    to: "https://slack.datahubproject.io",
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
