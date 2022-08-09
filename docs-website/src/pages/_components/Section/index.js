import React from "react";
import clsx from "clsx";
import useBaseUrl from "@docusaurus/useBaseUrl";
import styles from "./section.module.scss";

const Section = ({ title, children, withBackground }) => (
  <section className={clsx(styles.section, withBackground && styles.withBackground)}>
    <div className="container">
      <h2 className={styles.sectionTitle}>{title}</h2>
    </div>
    {children}
  </section>
);

const PromoSection = () => (
  <section className={clsx(styles.section, styles.promoSection)}>
    <div className="container">
      <img src={useBaseUrl("/img/acryl-logo-white-mark.svg")} />
      <h2>Managed DataHub</h2>
      <p>Acryl Data delivers an easy to consume DataHub platform for the enterprise</p>
      <a href="https://www.acryldata.io/datahub-beta" target="_blank" className="button button--primary button--lg">
        Sign up for Managed DataHub →
      </a>
    </div>
  </section>
);

export { Section, PromoSection };
