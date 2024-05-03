import React from "react";
import clsx from "clsx";
import styles from "./cardCTAs.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";
import { ArrowRightOutlined } from "@ant-design/icons";

const cardsContent = [
  {
    label: "Data Mesh",
    title: "Data Products, Delivered",
    url: "https://www.acryldata.io/blog/data-products-in-datahub-everything-you-need-to-know?utm_source=datahub&utm_medium=referral&utm_content=blog",
  },
  {
    label: "Data Contracts",
    title: "Data Contracts: End-to-end Reliability in Data",
    url: "https://www.acryldata.io/blog/data-contracts-in-datahub-combining-verifiability-with-holistic-data-management?utm_source=datahub&utm_medium=referral&utm_content=blog",
  },
  {
    label: "Shift Left",
    title: "Data Governance and Lineage Impact Analysis",
    url: "https://www.acryldata.io/blog/the-3-must-haves-of-metadata-management-part-2?utm_source=datahub&utm_medium=referral&utm_content=blog",
  },
];

const Card = ({ label, title, url }) => {
  return (
    <div className={clsx("col col--4 flex", styles.flexCol)}>
      <a href={url} target="_blank" className={clsx("card", styles.ctaCard)}>
        <div>
          <strong>{label}</strong>
          <h3 className={styles.ctaHeading}>{title}&nbsp;</h3>
        </div>
        <ArrowRightOutlined />
      </a>
    </div>
  );
};

const CardCTAs = () =>
  cardsContent?.length > 0 ? (
    <div style={{ padding: "2vh 0" }}>
      <div className="container">
        <div className="row">
          {cardsContent.map((props, idx) => (
            <Card key={idx} {...props} />
          ))}
        </div>
      </div>
    </div>
  ) : null;

export default CardCTAs;
