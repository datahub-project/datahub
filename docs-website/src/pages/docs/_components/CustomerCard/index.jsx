import React from "react";
import clsx from "clsx";
import styles from "./customercard.module.scss";
import Link from "@docusaurus/Link";

const CustomerCard = ({ customer, title, imgUrl, description, to,}) => {
  return (
      <div className={clsx("card", styles.card)}>
          <div className={styles.card_img}>
              <img src={imgUrl} alt={customer} />
              <div className={styles.card_overlay_text}>
                <div class={styles.card_customer}> {customer}</div>
                <div class={styles.card_title}> {title}</div>
              </div>
          </div>
          <div className={styles.card_body}>
            <p class="card-text">{description}</p>
            <div className={styles.card_button}>
                <Link className="button button--secondary button--md" href={to} target="_blank">
                  Learn More About {customer}'s Story
                </Link>
            </div>
          </div>
      </div>
  );
};

export default CustomerCard;
