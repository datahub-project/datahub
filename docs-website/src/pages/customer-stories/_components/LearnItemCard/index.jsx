import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import { useBlogPost } from "@docusaurus/theme-common/internal";
import styles from "./styles.module.scss";

export default function LearnItemCard({ company }) {
  return (
    <div className={clsx("col col--4", styles.featureCol)}>
      <div className={clsx("card", styles.card)}>
        <div className={styles.card_image}>
          <img src={company.imageUrl} alt={company.name} />
        </div>
        <div className={clsx("card__body", styles.featureBody)}>
          <div>{company.description}</div>
        </div>
        <div className={styles.card_button}>
          <Link className="button button--secondary button--md" href={company.link} target="_blank">
            Discover {company.name}'s Story
          </Link>
        </div>
      </div>
    </div>
  );
}