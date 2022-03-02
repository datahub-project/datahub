import React from "react";
import clsx from "clsx";
import useBaseUrl from "@docusaurus/useBaseUrl";

import styles from "../styles/section.module.scss";

const AnnouncementSection = () => (
  <section className={clsx(styles.section, styles.announcementSection)}>
    <div className="container">
      <img src={useBaseUrl("/img/acryl-logo-white-mark.svg")} />
      <h2>Managed DataHub</h2>
      <p>
        Acryl Data delivers an easy to consume DataHub platform for the
        enterprise
      </p>
      <a
        href="https://www.acryldata.io/datahub-beta"
        target="_blank"
        class="button button--primary button--lg"
      >
        Sign up for Managed DataHub →
      </a>
    </div>
  </section>
);

export default AnnouncementSection;
