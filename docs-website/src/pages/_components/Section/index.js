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
      <h2>DataHub Cloud</h2>
      <p>Acryl Data provides a live demo of DataHub Cloud every Tuesday and Thursday</p>
      <a href="https://www.acryldata.io/weekly-demo-landing-page?utm_source=datahub&utm_medium=referral&utm_campaign=acryl_signup" target="_blank" className="button button--primary button--lg">
        Sign up for a live demo →
      </a>
    </div>
  </section>
);

export { Section, PromoSection };
