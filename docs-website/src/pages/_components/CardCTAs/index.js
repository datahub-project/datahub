import React from "react";
import clsx from "clsx";
import styles from "./cardCTAs.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";
import { ArrowRightOutlined } from "@ant-design/icons";

const cardsContent = [
  {
    label: "Data Mesh",
    title: "Data Products, Delivered",
    url: "https://www.acryldata.io/blog/data-products-in-datahub-everything-you-need-to-know",
  },
  {
    label: "Data Contracts",
    title: "End-to-end Reliability in Data",
    url: "https://www.acryldata.io/blog/data-contracts-in-datahub-combining-verifiability-with-holistic-data-management",
  },
  {
    label: "Shift Left",
    title: "Developer-friendly Data Governance",
    url: "https://www.acryldata.io/blog/the-3-must-haves-of-metadata-management-part-2",
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
