import React from "react";
import clsx from "clsx";
import styles from "./quickstartcards.module.scss";
import QuickstartCard from '../QuickstartCard'
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";
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

const quickstartContent =  [
{
    title: "Quickstart with DataHub",
    icon: "datahub-logo-color-mark",
    to: "docs/quickstart",
    color: '#FFF',
    fontColor: '#091013',
  },
{
    title: "Learn about Managed DataHub",
    icon: "acryl-logo-transparent-mark",
    to: "docs/managed-datahub/managed-datahub-overview",
    color: '#091013',
    fontColor: '#FFF',
}
]

const QuickstartCards = () => {
return (
    <div style={{ padding: "2vh 0" }}>
      <div className="container" style={{ padding: "0"}}>
        <div className="row row--no-gutters">
          {quickstartContent.map((props, idx) => (
            <QuickstartCard key={idx} {...props} />
          ))}
        </div>
      </div>
    </div>
  );
};

export default QuickstartCards;
