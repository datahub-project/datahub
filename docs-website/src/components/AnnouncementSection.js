import React from "react";
import clsx from "clsx";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";

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
      <Link
        to={useBaseUrl("/managed")}
        class="button button--primary button--lg"
      >
        Sign up for Managed DataHub →
      </Link>
    </div>
  </section>
);

export default AnnouncementSection;
