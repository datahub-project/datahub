import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";

const featuresContent = [
  {
    title: "Open Source",
    imageUrl: "/img/icons/open.svg",
    description: (
      <>
        DataHub was originally{" "}
        <Link to={"https://engineering.linkedin.com/blog/2019/data-hub"}>
          built at LinkedIn
        </Link>{" "}
        and subsequently{" "}
        <Link to={"https://github.com/datahub-project/datahub"}>open-sourced</Link>{" "}
        under the Apache 2.0 License. It now has a thriving community with over
        a hundred contributors, and is widely used at many companies.
      </>
    ),
  },
  {
    title: "Forward Looking Architecture",
    imageUrl: "/img/icons/architecture.svg",
    description: (
      <>
        DataHub follows a{" "}
        <Link
          to={
            "https://engineering.linkedin.com/blog/2020/datahub-popular-metadata-architectures-explained"
          }
        >
          push-based architecture
        </Link>
        , which means it's built for continuously changing metadata. The modular
        design lets it scale with data growth at any organization, from a single
        database under your desk to multiple data centers spanning the globe.
      </>
    ),
  },
  {
    title: "Massive Ecosystem",
    imageUrl: "/img/icons/ecosystem.svg",
    description: (
      <>
        DataHub has pre-built integrations with your favorite systems: Kafka,
        Airflow, MySQL, SQL Server, Postgres, LDAP, Snowflake, Hive, BigQuery,
        and <Link to={"docs/metadata-ingestion"}>many others</Link>. The
        community is continuously adding more integrations, so this list keeps
        getting longer and longer.
      </>
    ),
  },
];

const Feature = ({ imageUrl, title, description }) => {
  return (
    <div className={clsx("col col--4")}>
      <div>
        <img
          src={useBaseUrl(imageUrl)}
          alt={title}
          className="invert-on-dark"
        />
      </div>
      <h3>{title}</h3>
      <p>{description}</p>
    </div>
  );
};

const Features = ({}) =>
  featuresContent && featuresContent.length > 0 ? (
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
