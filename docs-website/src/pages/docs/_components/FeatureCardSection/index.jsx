import React from "react";
import clsx from "clsx";
import styles from "./featurecardsection.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";
import FeatureCard from '../FeatureCard'
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

const featureCardContent =  [
{
    title: "Data Discovery",
    description: "Search your entire data ecosystem, including dashboards, datasets, ML models, and raw files.",
  },
{
    title: "Data Governance",
    description: "Define ownership and track PII.",
},
{
    title: "Data Quality Control",
    description: "Improve data quality through metadata tests, assertions, data freshness checks, and data contracts.",
},
{
  title: "UI-based Ingestion →",
  description: "Easily set up integrations in minutes using DataHub's intuitive UI-based ingestion feature.",
  to: "docs/ui-ingestion",
},
{
  title: "APIs and SDKs →",
  description: "For users who prefer programmatic control, DataHub offers a comprehensive set of APIs and SDKs.",
  to: "docs/api/datahub-apis",
},
{
  title: "Vibrant Community →",
  description: "Our community provides support through office hours, workshops, and a Slack channel.",
  to: "docs/community",
}
]

const FeatureCards = () => {
return (
    <div style={{ padding: "2vh 0" }}>
      <div className="container" style={{ padding: "0"}}>
        <div className="row row--no-gutters">
          {featureCardContent.map((props, idx) => (
            <FeatureCard key={idx} {...props} />
          ))}
        </div>
      </div>

    </div>
  );
};

export default FeatureCards;
