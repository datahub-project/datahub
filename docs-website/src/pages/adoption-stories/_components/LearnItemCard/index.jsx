import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import styles from "./styles.module.scss";

export default function LearnItemCard({ company }) {
  return (
    <div className={clsx("col col--4", styles.featureCol)}>
      <div className={clsx("card", styles.card)} id={company.slug}>
        <div className={styles.card_image}>
          <img src={`/img/adoption-stories/adoption-stories-${company.slug}.png`} alt={company.name} />
        </div>
        <div className={clsx("card__body", styles.featureBody)}>
          <div dangerouslySetInnerHTML={{ __html: company.description }} /></div>
        <div className={styles.card_button}>
          <Link className="button button--secondary button--md" href={company.link} target="_blank">
            Discover {company.name}'s Story
          </Link>
        </div>
      </div>
    </div>
  );
}
