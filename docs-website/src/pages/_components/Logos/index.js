import clsx from "clsx";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";
import React from "react";
import { Swiper, SwiperSlide } from "swiper/react";
import "swiper/css";
import "swiper/css/pagination";
import { Pagination } from "swiper/modules";
import styles from "./logos.module.scss";

const companies = [
  {
    name: "Airtel",
    imageUrl: "/img/logos/companies/airtel.png",
    imageSize: "large",
    link: "https://www.youtube.com/watch?v=yr24mM91BN4&list=PLdCtLs64vZvGCKMQC2dJEZ6cUqWsREbFi&index=9",
    category: "B2B & B2C",
  },
  {
    name: "Coursera",
    imageUrl: "/img/logos/companies/coursera.svg",
    imageSize: "small",
    link: "https://www.youtube.com/watch?v=bd5v4fn4d4s",
    category: "B2B & B2C",
  },
  {
    name: "Zynga",
    imageUrl: "/img/logos/companies/zynga.png",
    imageSize: "default",
    link: "https://www.youtube.com/watch?v=VCU3-Hd_glI",
    category: "B2B & B2C",
  },
  {
    name: "Geotab",
    imageUrl: "/img/logos/companies/geotab.jpg",
    imageSize: "small",
    link: "https://www.youtube.com/watch?v=boyjT2OrlU4",
    category: "B2B & B2C",
  },
  {
    name: "Hurb",
    imageUrl: "/img/logos/companies/hurb.png",
    imageSize: "medium",
    link: "https://www.youtube.com/watch?v=G-xOe5OGGw4",
    category: "B2B & B2C",
  },
  {
    name: "Saxo Bank",
    imageUrl: "/img/logos/companies/saxobank.svg",
    imageSize: "default",
    link: "https://www.youtube.com/watch?v=8EsgE8urqHI",
    category: "Financial & Fintech",
  },
  {
    name: "Adevinta",
    imageUrl: "/img/logos/companies/adevinta.png",
    imageSize: "medium",
    link: "https://www.youtube.com/watch?v=u9DRa_5uPIM",
    category: "E-Commerce",
  },
  {
    name: "Grofers",
    imageUrl: "/img/logos/companies/grofers.png",
    imageSize: "medium",
    link: "https://www.youtube.com/watch?v=m9kUYAuezFI",
    category: "E-Commerce",
  },
  {
    name: "Wolt",
    imageUrl: "/img/logos/companies/wolt.png",
    imageSize: "default",
    link: "https://www.youtube.com/watch?v=D8XsfoZuwt0&t=75s",
    category: "E-Commerce",
  },
  {
    name: "Viasat",
    imageUrl: "/img/logos/companies/viasat.png",
    imageSize: "medium",
    link: "https://www.youtube.com/watch?v=2SrDAJnzkjE",
    category: "And More",
  },
  {
    name: "Optum",
    imageUrl: "/img/logos/companies/optum.jpg",
    imageSize: "medium",
    link: "https://www.youtube.com/watch?v=NuLLc88ij-s",
    category: "And More",
  },
  {
    name: "LinkedIn",
    imageUrl: "/img/logos/companies/linkedin.svg",
    imageSize: "medium",
    category: "B2B & B2C",
  },
  {
    name: "Udemy",
    imageUrl: "/img/logos/companies/udemy.png",
    imageSize: "medium",
    category: "B2B & B2C",
  },
  {
    name: "ThoughtWorks",
    imageUrl: "/img/logos/companies/thoughtworks.png",
    imageSize: "medium",
    category: "B2B & B2C",
  },
  {
    name: "Expedia Group",
    imageUrl: "/img/logos/companies/expedia.svg",
    imageSize: "medium",
    category: "B2B & B2C",
  },
  {
    name: "Typeform",
    imageUrl: "/img/logos/companies/typeform.svg",
    imageSize: "medium",
    category: "B2B & B2C",
  },
  {
    name: "Peloton",
    imageUrl: "/img/logos/companies/peloton.png",
    imageSize: "default",
    category: "B2B & B2C",
  },
  {
    name: "Razer",
    imageUrl: "/img/logos/companies/razer.jpeg",
    imageSize: "large",
    category: "B2B & B2C",
  },
  {
    name: "ClassDojo",
    imageUrl: "/img/logos/companies/classdojo.png",
    imageSize: "medium",
    category: "B2B & B2C",
  },
  {
    name: "Klarna",
    imageUrl: "/img/logos/companies/klarna.svg",
    imageSize: "medium",
    category: "Financial & Fintech",
  },
  {
    name: "N26",
    imageUrl: "/img/logos/companies/n26.svg",
    imageSize: "medium",
    category: "Financial & Fintech",
  },
  {
    name: "BankSalad",
    imageUrl: "/img/logos/companies/banksalad.png",
    imageSize: "default",
    category: "Financial & Fintech",
  },
  {
    name: "Uphold",
    imageUrl: "/img/logos/companies/uphold.png",
    imageSize: "default",
    category: "Financial & Fintech",
  },
  {
    name: "Stash",
    imageUrl: "/img/logos/companies/stash.svg",
    imageSize: "medium",
    category: "Financial & Fintech",
  },
  {
    name: "SumUp",
    imageUrl: "/img/logos/companies/sumup.png",
    imageSize: "medium",
    category: "Financial & Fintech",
  },
  {
    name: "VanMoof",
    imageUrl: "/img/logos/companies/vanmoof.png",
    imageSize: "small",
    category: "E-Commerce",
  },
  {
    name: "SpotHero",
    imageUrl: "/img/logos/companies/spothero.png",
    imageSize: "default",
    category: "E-Commerce",
  },
  {
    name: "hipages",
    imageUrl: "/img/logos/companies/hipages.png",
    imageSize: "medium",
    category: "E-Commerce",
  },
  {
    name: "Showroomprive.com",
    imageUrl: "/img/logos/companies/showroomprive.png",
    imageSize: "small",
    category: "E-Commerce",
  },
  {
    name: "Wikimedia Foundation",
    imageUrl: "/img/logos/companies/wikimedia-foundation.png",
    imageSize: "medium",
    category: "And More",
  },
  {
    name: "Cabify",
    imageUrl: "/img/logos/companies/cabify.png",
    imageSize: "medium",
    category: "And More",
  },
  {
    name: "Digital Turbine",
    imageUrl: "/img/logos/companies/digitalturbine.svg",
    imageSize: "medium",
    category: "And More",
  },
  {
    name: "DFDS",
    imageUrl: "/img/logos/companies/dfds.png",
    imageSize: "medium",
    category: "And More",
  },
  {
    name: "Moloco",
    imageUrl: "/img/logos/companies/moloco.png",
    imageSize: "medium",
    category: "And More",
  }
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
      {companies.map((company, idx) => (
        <SwiperSlide key={idx}>
          {company.link ? (
            <a
              href={company.link}
              target="_blank"
              rel="noopener noreferrer"
            >
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
    <div className={clsx(styles.buttonContainer)}>
    <Link
      className="button button--secondary button--md"
      to={useBaseUrl("docs/what-is-datahub/customer-stories")}
    >
      Check Out Adoption Stories →
    </Link>
  </div>
  </div>
);
