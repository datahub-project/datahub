import React from "react";
import useBaseUrl from "@docusaurus/useBaseUrl";

import styles from "./logos.module.scss";


const companyIndexes = require("../../../../adoptionStoriesIndexes.json");
const companies = companyIndexes.companies;



export const CompanyLogos = () => (
  <div className={styles.marquee}>
    <div>
      {[...companies, ...companies].map((logo, idx) => (
        <img src={useBaseUrl(logo.imageUrl)} alt={logo.name} title={logo.name} key={idx} className={styles.platformLogo} />
      ))}
    </div>
  </div>
);

export default CompanyLogos;