import clsx from "clsx";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";
import React from "react";
import { Swiper, SwiperSlide } from "swiper/react";
import "swiper/css";
import "swiper/css/pagination";
import { Pagination } from "swiper/modules";
import styles from "./logos.module.scss";
const companyIndexes = require("../../../../adoptionStoriesIndexes.json");
const companies = companyIndexes.companies;


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
    name: "Azure AD",
    imageUrl: "/img/logos/platforms/azure-ad.png",
  },
  {
    name: "BigQuery",
    imageUrl: "/img/logos/platforms/bigquery.svg",
  },
  {
    name: "Clickhouse",
    imageUrl: "/img/logos/platforms/clickhouse.svg",
  },
  {
    name: "CouchBase",
    imageUrl: "/img/logos/platforms/couchbase.svg",
  },
  { name: "Dagster", imageUrl: "/img/logos/platforms/dagster.png" },
  { name: "Databricks", imageUrl: "/img/logos/platforms/databricks.png" },
  { name: "DBT", imageUrl: "/img/logos/platforms/dbt.svg" },
  { name: "Deltalake", imageUrl: "/img/logos/platforms/deltalake.svg" },
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
    name: "Great Expectations",
    imageUrl: "/img/logos/platforms/great-expectations.png",
  },
  {
    name: "Hadoop",
    imageUrl: "/img/logos/platforms/hadoop.svg",
  },
  {
    name: "Hive",
    imageUrl: "/img/logos/platforms/hive.svg",
  },
  { name: "Iceberg", imageUrl: "/img/logos/platforms/iceberg.png" },
  { name: "Kafka", imageUrl: "/img/logos/platforms/kafka.svg" },
  { name: "Kusto", imageUrl: "/img/logos/platforms/kusto.svg" },
  { name: "Looker", imageUrl: "/img/logos/platforms/looker.svg" },
  { name: "MariaDB", imageUrl: "/img/logos/platforms/mariadb.png" },
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
  { name: "NiFi", imageUrl: "/img/logos/platforms/nifi.svg" },
  { name: "Okta", imageUrl: "/img/logos/platforms/oracle.svg" },
  { name: "Oracle", imageUrl: "/img/logos/platforms/okta.png" },
  { name: "Pinot", imageUrl: "/img/logos/platforms/pinot.svg" },
  { name: "PostgreSQL", imageUrl: "/img/logos/platforms/postgres.svg" },
  { name: "PowerBI", imageUrl: "/img/logos/platforms/powerbi.png" },
  { name: "Prefect", imageUrl: "/img/logos/platforms/prefect.svg" },
  { name: "Presto", imageUrl: "/img/logos/platforms/presto.svg" },
  { name: "Protobuf", imageUrl: "/img/logos/platforms/protobuf.png" },
  { name: "Pulsar", imageUrl: "/img/logos/platforms/pulsar.png" },
  { name: "Redash", imageUrl: "/img/logos/platforms/redash.svg" },
  {
    name: "Redshift",
    imageUrl: "/img/logos/platforms/redshift.svg",
  },
  {
    name: "S3",
    imageUrl: "/img/logos/platforms/s3.svg",
  },
  { name: "Salesforce", imageUrl: "/img/logos/platforms/salesforce.png" },
  {
    name: "SageMaker",
    imageUrl: "/img/logos/platforms/sagemaker.svg",
  },
  { name: "Snowflake", imageUrl: "/img/logos/platforms/snowflake.svg" },
  { name: "Spark", imageUrl: "/img/logos/platforms/spark.svg" },
  { name: "SQLAlchemy", imageUrl: "/img/logos/platforms/sqlalchemy.png" },
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
  {
    name: "Trino",
    imageUrl: "/img/logos/platforms/trino.png",
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
  <div className={clsx("container", styles.companyLogoContainer)}>
    <Swiper
      slidesPerView={8}
      spaceBetween={30}
      slidesPerGroup={6}
      breakpoints={{
        320: {
          slidesPerView: 2,
          spaceBetween: 10,
        },
        480: {
          slidesPerView: 4,
          spaceBetween: 20,
        },
        960: {
          slidesPerView: 8,
          spaceBetween: 30,
        },
      }}
      pagination={{ clickable: true }}
      modules={[Pagination]}
      className={clsx("mySwiper", styles.companyWrapper)}
    >
      {companies
        .filter((company) => company.imageUrl && company.link) // Filter companies with imageUrl and link
        .map((company, idx) => (
          <SwiperSlide key={idx}>
            {company.link ? (
              <a href={`/adoption-stories#${company.slug}`}>
                <img
                  src={useBaseUrl(company.imageUrl)}
                  alt={company.name}
                  title={company.name}
                  className={clsx(
                    styles.companyLogo,
                    styles.companyLogoWithLink,
                    styles[company.imageSize]
                  )}
                />
              </a>
            ) : (
              <img
                src={useBaseUrl(company.imageUrl)}
                alt={company.name}
                title={company.name}
                className={clsx(
                  styles.companyLogo,
                  styles[company.imageSize]
                )}
              />
            )}
          </SwiperSlide>
        ))}
    </Swiper>
  </div>
);
