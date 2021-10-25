import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";

import styles from "../styles/logos.module.scss";

const companyLogos = [
  {
    name: "LinkedIn",
    imageUrl: "/img/logos/companies/linkedin.svg",
    size: "small",
  },
  {
    name: "Expedia Group",
    imageUrl: "/img/logos/companies/expedia.svg",
    size: "default",
  },
  {
    name: "Saxo Bank",
    imageUrl: "/img/logos/companies/saxobank.svg",
    size: "default",
  },
  {
    name: "Grofers",
    imageUrl: "/img/logos/companies/grofers.png",
    size: "default",
  },
  {
    name: "Typeform",
    imageUrl: "/img/logos/companies/typeform.svg",
    size: "default",
  },
  {
    name: "Peloton",
    imageUrl: "/img/logos/companies/peloton.png",
    size: "large",
  },
  {
    name: "SpotHero",
    imageUrl: "/img/logos/companies/spothero.png",
    size: "default",
  },
  {
    name: "Geotab",
    imageUrl: "/img/logos/companies/geotab.jpg",
    size: "small",
  },
  {
    name: "ThoughtWorks",
    imageUrl: "/img/logos/companies/thoughtworks.png",
    size: "large",
  },
  {
    name: "Viasat",
    imageUrl: "/img/logos/companies/viasat.png",
    size: "large",
  },
  {
    name: "Klarna",
    imageUrl: "/img/logos/companies/klarna.svg",
    size: "small",
  },
  {
    name: "Wolt",
    imageUrl: "/img/logos/companies/wolt.png",
    size: "large",
  },
  {
    name: "DFDS",
    imageUrl: "/img/logos/companies/dfds.png",
    size: "default",
  },
  {
    name: "BankSalad",
    imageUrl: "/img/logos/companies/banksalad.png",
    size: "large",
  },
  {
    name: "Uphold",
    imageUrl: "/img/logos/companies/uphold.png",
    size: "large",
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
  { name: "MongoDB", imageUrl: "/img/logos/platforms/mongodb.svg" },
  {
    name: "MSSQL",
    imageUrl: "/img/logos/platforms/mssql.svg",
  },
  {
    name: "MySQL",
    imageUrl: "/img/logos/platforms/mysql.svg",
  },
  { name: "Oracle", imageUrl: "/img/logos/platforms/oracle.svg" },
  { name: "Pinot", imageUrl: "/img/logos/platforms/pinot.svg" },
  { name: "PostgreSQL", imageUrl: "/img/logos/platforms/postgres.svg" },
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
    name: "Teradata",
    imageUrl: "/img/logos/platforms/teradata.svg",
  },
];

export const PlatformLogos = () => (
  <Link
    to={useBaseUrl("docs/metadata-ingestion#installing-plugins/")}
    className={styles.marquee}
  >
    <div>
      {[...platformLogos, ...platformLogos].map((logo, idx) => (
        <img
          src={useBaseUrl(logo.imageUrl)}
          alt={logo.name}
          title={logo.name}
          key={idx}
          className={styles.platformLogo}
        />
      ))}
    </div>
  </Link>
);

export const CompanyLogos = () => (
  <div className={styles.marquee}>
    <div className={styles.companyWrapper}>
      {[...companyLogos, ...companyLogos].map((logo, idx) => (
        <img
          src={useBaseUrl(logo.imageUrl)}
          alt={logo.name}
          title={logo.name}
          key={idx}
          className={clsx(styles.companyLogo, styles[logo.size])}
        />
      ))}
    </div>
  </div>
);
