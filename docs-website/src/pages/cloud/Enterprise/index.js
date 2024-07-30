import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import styles from "./styles.module.scss";


const featuresContent = [
  {
    title: "99.5% Uptime SLA",
    description: "We’ll focus on keeping DataHub running, so you can focus on the things that really matter."
  },
  {
    title: "In-VPC Ingestion & Check execution",
    description: "So your actual data never leaves your network."
  },
  {
    title: "SOC-2 Compliance",
    description: "An incredibly high level of security you can trust."
  },
];

const Feature = ({ title, description }) => {
  return (
    <div className={clsx(styles.feature, "col col--4")}>
      <h2>{title}</h2>
      <p>{description}</p>
    </div>
  );
};

const Features = () =>
  featuresContent?.length > 0 ? (
    <div style={{ padding: "2vh 6vh" }}>
      <div className={"container"} style={{ width: "90%"}}>
        <div className={clsx(styles.title)}>Enterprise Ready</div>
        <div className="row">
          {featuresContent.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
        <div className={clsx(styles.moreBenefits)}>
          +4 Benefits
        </div>
      </div>
    </div>
  ) : null;

export default Features;
