import React from "react";
import clsx from "clsx";
import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";

import styles from "./logos.module.scss";

const companiesByIndustry = [
  {
    name: "B2B & B2C",
    companies: [
      {
        name: "LinkedIn",
        imageUrl: "/img/logos/companies/linkedin.svg",
        imageSize: "medium",
      },
      {
        name: "Udemy",
        imageUrl: "/img/logos/companies/udemy.png",
        imageSize: "medium",
      },
      {
        name: "ClassDojo",
        imageUrl: "/img/logos/companies/classdojo.png",
        imageSize: "medium",
      },
      {
        name: "Coursera",
        imageUrl: "/img/logos/companies/coursera.svg",
        imageSize: "small",
      },
      {
        name: "Geotab",
        imageUrl: "/img/logos/companies/geotab.jpg",
        imageSize: "small",
      },
      {
        name: "ThoughtWorks",
        imageUrl: "/img/logos/companies/thoughtworks.png",
        imageSize: "medium",
      },
      {
        name: "Expedia Group",
        imageUrl: "/img/logos/companies/expedia.svg",
        imageSize: "medium",
      },
      {
        name: "Typeform",
        imageUrl: "/img/logos/companies/typeform.svg",
        imageSize: "medium",
      },
      {
        name: "Peloton",
        imageUrl: "/img/logos/companies/peloton.png",
        imageSize: "default",
      },
      {
        name: "Zynga",
        imageUrl: "/img/logos/companies/zynga.png",
        imageSize: "default",
      },
      {
        name: "Hurb",
        imageUrl: "/img/logos/companies/hurb.png",
        imageSize: "medium",
      },
      {
        name: "Razer",
        imageUrl: "/img/logos/companies/razer.jpeg",
        imageSize: "large",
      },
    ],
  },
  {
    name: "Financial & Fintech",
    companies: [
      {
        name: "Saxo Bank",
        imageUrl: "/img/logos/companies/saxobank.svg",
        imageSize: "default",
      },
      {
        name: "Klarna",
        imageUrl: "/img/logos/companies/klarna.svg",
        imageSize: "medium",
      },
      {
        name: "N26",
        imageUrl: "/img/logos/companies/n26.svg",
        imageSize: "medium",
      },
      {
        name: "BankSalad",
        imageUrl: "/img/logos/companies/banksalad.png",
        imageSize: "default",
      },
      {
        name: "Uphold",
        imageUrl: "/img/logos/companies/uphold.png",
        imageSize: "default",
      },
      {
        name: "Stash",
        imageUrl: "/img/logos/companies/stash.svg",
        imageSize: "medium",
      },
      {
        name: "SumUp",
        imageUrl: "/img/logos/companies/sumup.png",
        imageSize: "medium",
      },
    ],
  },
  {
    name: "E-Commerce",
    companies: [
      {
        name: "Adevinta",
        imageUrl: "/img/logos/companies/adevinta.png",
        imageSize: "medium",
      },
      {
        name: "Grofers",
        imageUrl: "/img/logos/companies/grofers.png",
        imageSize: "medium",
      },
      {
        name: "SpotHero",
        imageUrl: "/img/logos/companies/spothero.png",
        imageSize: "default",
      },
      {
        name: "hipages",
        imageUrl: "/img/logos/companies/hipages.png",
        imageSize: "medium",
      },
      {
        name: "Wolt",
        imageUrl: "/img/logos/companies/wolt.png",
        imageSize: "default",
      },
      {
        name: "Showroomprive.com",
        imageUrl: "/img/logos/companies/showroomprive.png",
        imageSize: "small",
      },
    ],
  },
  {
    name: "And More",
    companies: [
      {
        name: "Wikimedia Foundation",
        imageUrl: "/img/logos/companies/wikimedia-foundation.png",
        imageSize: "medium",
      },
      {
        name: "Cabify",
        imageUrl: "/img/logos/companies/cabify.png",
        imageSize: "medium",
      },
      {
        name: "Digital Turbine",
        imageUrl: "/img/logos/companies/digitalturbine.svg",
        imageSize: "medium",
      },
      {
        name: "Viasat",
        imageUrl: "/img/logos/companies/viasat.png",
        imageSize: "medium",
      },
      {
        name: "DFDS",
        imageUrl: "/img/logos/companies/dfds.png",
        imageSize: "medium",
      },
      {
        name: "Moloco",
        imageUrl: "/img/logos/companies/moloco.png",
        imageSize: "medium",
      },
      {
        name: "Optum",
        imageUrl: "/img/logos/companies/optum.jpg",
        imageSize: "medium",
      },
    ],
  },
];

const platformLogos = [
  {
    name: "ADLS",
    imageUrl: "/img/logos/platforms/adls.svg",
  },
  {
    name: "Airflow",
    imageUrl: "/img/logos/platforms/airflow.svg",
  },
  {
    name: "Athena",
    imageUrl: "/img/logos/platforms/athena.svg",
  },
  {
    name: "BigQuery",
    imageUrl: "/img/logos/platforms/bigquery.svg",
  },
  {
    name: "CouchBase",
    imageUrl: "/img/logos/platforms/couchbase.svg",
  },
  { name: "DBT", imageUrl: "/img/logos/platforms/dbt.svg" },
  { name: "Druid", imageUrl: "/img/logos/platforms/druid.svg" },
  { name: "Elasticsearch", imageUrl: "/img/logos/platforms/elasticsearch.svg" },
  {
    name: "Feast",
    imageUrl: "/img/logos/platforms/feast.svg",
  },
  {
    name: "Glue",
    imageUrl: "/img/logos/platforms/glue.svg",
  },
  {
    name: "Hadoop",
    imageUrl: "/img/logos/platforms/hadoop.svg",
  },
  {
    name: "Hive",
    imageUrl: "/img/logos/platforms/hive.svg",
  },
  { name: "Kafka", imageUrl: "/img/logos/platforms/kafka.svg" },
  { name: "Kusto", imageUrl: "/img/logos/platforms/kusto.svg" },
  { name: "Looker", imageUrl: "/img/logos/platforms/looker.svg" },
  { name: "Metabase", imageUrl: "/img/logos/platforms/metabase.svg" },
  { name: "Mode", imageUrl: "/img/logos/platforms/mode.png" },
  { name: "MongoDB", imageUrl: "/img/logos/platforms/mongodb.svg" },
  {
    name: "MSSQL",
    imageUrl: "/img/logos/platforms/mssql.svg",
  },
  {
    name: "MySQL",
    imageUrl: "/img/logos/platforms/mysql.svg",
  },
  { name: "Nifi", imageUrl: "/img/logos/platforms/nifi.svg" },
  { name: "Oracle", imageUrl: "/img/logos/platforms/oracle.svg" },
  { name: "Pinot", imageUrl: "/img/logos/platforms/pinot.svg" },
  { name: "PostgreSQL", imageUrl: "/img/logos/platforms/postgres.svg" },
  { name: "PowerBI", imageUrl: "/img/logos/platforms/powerbi.png" },
  { name: "Presto", imageUrl: "/img/logos/platforms/presto.svg" },
  { name: "Redash", imageUrl: "/img/logos/platforms/redash.svg" },
  {
    name: "Redshift",
    imageUrl: "/img/logos/platforms/redshift.svg",
  },
  {
    name: "S3",
    imageUrl: "/img/logos/platforms/s3.svg",
  },
  {
    name: "SageMaker",
    imageUrl: "/img/logos/platforms/sagemaker.svg",
  },
  { name: "Snowflake", imageUrl: "/img/logos/platforms/snowflake.svg" },
  { name: "Spark", imageUrl: "/img/logos/platforms/spark.svg" },
  {
    name: "Superset",
    imageUrl: "/img/logos/platforms/superset.svg",
  },
  {
    name: "Tableau",
    imageUrl: "/img/logos/platforms/tableau.png",
  },
  {
    name: "Teradata",
    imageUrl: "/img/logos/platforms/teradata.svg",
  },
];

export const PlatformLogos = () => (
  <Link to={useBaseUrl("docs/metadata-ingestion#installing-plugins/")} className={styles.marquee}>
    <div>
      {[...platformLogos, ...platformLogos].map((logo, idx) => (
        <img src={useBaseUrl(logo.imageUrl)} alt={logo.name} title={logo.name} key={idx} className={styles.platformLogo} />
      ))}
    </div>
  </Link>
);

export const CompanyLogos = () => (
  <div className={clsx("container", styles.companyLogoContainer)}>
    <Tabs className="pillTabs">
      {companiesByIndustry.map((industry, idx) => (
        <TabItem value={`industry-${idx}`} label={industry.name} key={idx} default={idx === 0}>
          <div className={styles.companyWrapper}>
            {industry.companies.map((company, idx) => (
              <img
                src={useBaseUrl(company.imageUrl)}
                alt={company.name}
                title={company.name}
                key={idx}
                className={clsx(styles.companyLogo, styles[company.imageSize])}
              />
            ))}
          </div>
        </TabItem>
      ))}
    </Tabs>
  </div>
);
