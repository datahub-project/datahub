import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";

import styles from "../styles/logos.module.scss";

const companyLogos = [
  {
    name: "LinkedIn",
    imageUrl: "/static/img/logos/companies/linkedin.svg",
    size: "small",
  },
  {
    name: "Expedia Group",
    imageUrl: "/static/img/logos/companies/expedia.svg",
    size: "default",
  },
  {
    name: "Saxo Bank",
    imageUrl: "/static/img/logos/companies/saxobank.svg",
    size: "default",
  },
  {
    name: "Grofers",
    imageUrl: "/static/img/logos/companies/grofers.png",
    size: "default",
  },
  {
    name: "Typeform",
    imageUrl: "/static/img/logos/companies/typeform.svg",
    size: "default",
  },
  {
    name: "SpotHero",
    imageUrl: "/static/img/logos/companies/spothero.png",
    size: "default",
  },
  {
    name: "Geotab",
    imageUrl: "/static/img/logos/companies/geotab.jpg",
    size: "small",
  },
  {
    name: "ThoughtWorks",
    imageUrl: "/static/img/logos/companies/thoughtworks.png",
    size: "large",
  },
  {
    name: "Viasat",
    imageUrl: "/static/img/logos/companies/viasat.png",
    size: "large",
  },
  {
    name: "Klarna",
    imageUrl: "/static/img/logos/companies/klarna.svg",
    size: "small",
  },
  {
    name: "Wolt",
    imageUrl: "/static/img/logos/companies/wolt.png",
    size: "large",
  },
  {
    name: "DFDS",
    imageUrl: "/static/img/logos/companies/dfds.png",
    size: "default",
  },
  {
    name: "BankSalad",
    imageUrl: "/static/img/logos/companies/banksalad.png",
    size: "large",
  },
  {
    name: "Uphold",
    imageUrl: "/static/img/logos/companies/uphold.png",
    size: "large",
  },
];

const platformLogos = [
  {
    name: "ADLS",
    imageUrl: "/static/img/logos/platforms/adls.svg",
  },
  {
    name: "Airflow",
    imageUrl: "/static/img/logos/platforms/airflow.svg",
  },
  {
    name: "Athena",
    imageUrl: "/static/img/logos/platforms/athena.svg",
  },
  {
    name: "BigQuery",
    imageUrl: "/static/img/logos/platforms/bigquery.svg",
  },
  {
    name: "CouchBase",
    imageUrl: "/static/img/logos/platforms/couchbase.svg",
  },
  { name: "DBT", imageUrl: "/static/img/logos/platforms/dbt.svg" },
  { name: "Druid", imageUrl: "/static/img/logos/platforms/druid.svg" },
  {
    name: "Feast",
    imageUrl: "/static/img/logos/platforms/feast.svg",
  },
  {
    name: "Glue",
    imageUrl: "/static/img/logos/platforms/glue.svg",
  },
  {
    name: "Hadoop",
    imageUrl: "/static/img/logos/platforms/hadoop.svg",
  },
  {
    name: "Hive",
    imageUrl: "/static/img/logos/platforms/hive.svg",
  },
  { name: "Kafka", imageUrl: "/static/img/logos/platforms/kafka.svg" },
  { name: "Kusto", imageUrl: "/static/img/logos/platforms/kusto.svg" },
  { name: "Looker", imageUrl: "/static/img/logos/platforms/looker.svg" },
  { name: "MongoDB", imageUrl: "/static/img/logos/platforms/mongodb.svg" },
  {
    name: "MSSQL",
    imageUrl: "/static/img/logos/platforms/mssql.svg",
  },
  {
    name: "MySQL",
    imageUrl: "/static/img/logos/platforms/mysql.svg",
  },
  { name: "Oracle", imageUrl: "/static/img/logos/platforms/oracle.svg" },
  { name: "Pinot", imageUrl: "/static/img/logos/platforms/pinot.svg" },
  { name: "PostgreSQL", imageUrl: "/static/img/logos/platforms/postgres.svg" },
  { name: "Presto", imageUrl: "/static/img/logos/platforms/presto.svg" },
  { name: "Redash", imageUrl: "/static/img/logos/platforms/redash.svg" },
  {
    name: "Redshift",
    imageUrl: "/static/img/logos/platforms/redshift.svg",
  },
  {
    name: "S3",
    imageUrl: "/static/img/logos/platforms/s3.svg",
  },
  {
    name: "SageMaker",
    imageUrl: "/static/img/logos/platforms/sagemaker.svg",
  },
  { name: "Snowflake", imageUrl: "/static/img/logos/platforms/snowflake.svg" },
  { name: "Spark", imageUrl: "/static/img/logos/platforms/spark.svg" },
  {
    name: "Superset",
    imageUrl: "/static/img/logos/platforms/superset.svg",
  },
  {
    name: "Teradata",
    imageUrl: "/static/img/logos/platforms/teradata.svg",
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
  <Link
    to={useBaseUrl("docs/metadata-ingestion#installing-plugins/")}
    className={styles.marquee}
  >
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
  </Link>
);
