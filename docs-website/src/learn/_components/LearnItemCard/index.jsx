import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import { useBlogPost } from "@docusaurus/theme-common/internal";
import styles from "./styles.module.scss";

export default function LearnItemCard() {
  const { metadata } = useBlogPost();
  const { permalink, title, description, formattedDate, frontMatter } = metadata;
  return (
    <div className={clsx("col col--4", styles.featureCol)}>
      <Link to={permalink} className={clsx("card", styles.card)}>
        {frontMatter?.image ? (
          <div className={styles.card_image}>
            <img src={frontMatter?.image} alt={title} />
          </div>
        ) : (
          <div className={clsx("card__header", styles.featureHeader)}>
            <h2>{title}</h2>
          </div>
        )}
        <div className={clsx("card__body", styles.featureBody)}>
          <div>{description}</div>
        </div>

        <div className={clsx(styles.card_date)}>Published on {formattedDate}</div>
      </Link>
    </div>
  );
}