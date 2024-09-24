import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import styles from "./styles.module.scss";

function WebinarCard({ slug, title, excerpt, cardImg, link, status }) {
  return (
    <div className={clsx("col col--4", styles.featureCol)} id={slug}>
      <div className={clsx("card", styles.card)}>
        <div className={styles.card_image}>
          <img src={cardImg} alt={title} />
        </div>
        <div className={clsx("card__header", styles.featureHeader)}>
          <h4>{status}</h4>
          <h3>{title}</h3>
        </div>
        <div className={clsx("card__body", styles.featureBody)}>
          <br />
          <p>{excerpt}</p>
        </div>
        <div className={styles.card_button}>
          <Link
            className="button button--secondary button--md"
            to={`/webinars-poc/webinar/${slug}`}
          >
            Discover
          </Link>
        </div>
      </div>
    </div>
  );
}

export default WebinarCard;
