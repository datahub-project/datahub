import React from "react";
import clsx from "clsx";
import styles from "./customercard.module.scss";
import Link from "@docusaurus/Link";

const CustomerCard = ({ customer, title, imgUrl, description, to,}) => {
  return (
      <div className={clsx("card", styles.card)}>
      <div className={styles.card_img}>
        <img src={imgUrl} className="card-img-top" alt={customer} />
      </div>
      <div className={styles.card_body}>
        <h3 class="card-title">{customer} : {title}</h3>
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
