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
    title: "In-VPC Ingestion and<br/>Data Quality evaluation",
    description: "So your actual data never leaves your network."
  },
  {
    title: "SOC-2 Compliant",
    description: "An incredibly high level of security you can trust."
  },
];

const Feature = ({ title, description }) => {
  return (
    <div className={clsx(styles.feature, "col col--4")}>
      <h2 dangerouslySetInnerHTML={{ __html: title }}></h2>
      <p>{description}</p>
    </div>
  );
};

const Features = () =>
  featuresContent?.length > 0 ? (
    <div className={clsx(styles.container)}>
      <div className={clsx(styles.wrapper)}>
        <div className={clsx(styles.title)}>Enterprise Ready</div>
        <div className="row">
          {featuresContent.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
        <a href="/docs/managed-datahub/managed-datahub-overview#enterprise-grade" target="_blank" className={clsx(styles.moreBenefits)}>
          +4 benefits
        </a>
      </div>
    </div>
  ) : null;

export default Features;
