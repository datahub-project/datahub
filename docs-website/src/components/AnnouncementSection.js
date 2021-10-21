import React from "react";
import clsx from "clsx";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";

import styles from "../styles/section.module.scss";

const AcrylSection = () => (
  <section className={clsx(styles.section, styles.announcementSection)}>
    <div className="container">
      <img src={useBaseUrl("/static/img/acryl-logo-white-mark.svg")} />
      <h2>Managed DataHub</h2>
      <p>
        Acryl Data delivers an easy to consume DataHub platform for the
        enterprise
      </p>
      <Link
        to={useBaseUrl("/docs/saas")}
        target="_blank"
        class="button button--primary button--lg"
      >
        Sign up for Managed DataHub →
      </Link>
    </div>
  </section>
);

export default AcrylSection;

export const Logo = (props) => {
  return (
    <div style={{ display: "flex", justifyContent: "center", padding: "20px" }}>
      <img
        height="150"
        alt="DataHub Logo"
        src={useBaseUrl("/static/img/datahub-logo-color-mark.svg")}
        {...props}
      />
    </div>
  );
};
