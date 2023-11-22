import React from "react";
import FeatureCard from '../FeatureCard'
import {
  EyeTwoTone,
  HeartTwoTone,
  ApiTwoTone,
  AlertTwoTone,
  CompassTwoTone,
  ProfileTwoTone,
} from "@ant-design/icons";

const featureCardContent =  [
{
    title: "Data Discovery",
    description: "Search your entire data ecosystem, including dashboards, datasets, ML models, and raw files.",
    to: "docs/how/search",
    icon: <EyeTwoTone />
  },
{
    title: "Data Governance",
    description: "Define ownership and track PII.",
    to: "https://www.acryldata.io/blog/the-3-must-haves-of-metadata-management-part-2?utm_source=datahub&utm_medium=referral&utm_content=blog",
    icon: <CompassTwoTone />
  },
{
    title: "Data Quality Control",
    description: "Improve data quality through metadata tests, assertions, data freshness checks, and data contracts.",
    to: "https://www.acryldata.io/blog/data-contracts-in-datahub-combining-verifiability-with-holistic-data-management?utm_source=datahub&utm_medium=referral&utm_content=blog",
    icon: <AlertTwoTone />
  },
{
  title: "UI-based Ingestion",
  description: "Easily set up integrations in minutes using DataHub's intuitive UI-based ingestion feature.",
  to: "docs/ui-ingestion",
  icon: <ProfileTwoTone />
},
{
  title: "APIs and SDKs",
  description: "For users who prefer programmatic control, DataHub offers a comprehensive set of APIs and SDKs.",
  to: "docs/api/datahub-apis",
  icon: <ApiTwoTone />
},
{
  title: "Vibrant Community",
  description: "Our community provides support through office hours, workshops, and a Slack channel.",
  to: "docs/slack",
  icon: <HeartTwoTone />
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
