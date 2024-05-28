import React from "react";
import clsx from "clsx";
import styles from "./customercard.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";

const CustomerCard = ({ customer, title, description, to }) => {
  return (
    <div className="row g-0">
      <div className={clsx("card", styles.customer)}>
        <div className="col-md-4">
          <img src="..." className="img-fluid rounded-start" alt="IMG" />
        </div>
        <div className="col-md-8">
          <div className="card-body">
            <h3 className="card-title">{customer} : {title}</h3>
            <p className="card-text">{description}</p>
            <p className="card-text">
              <a href={to} class="btn btn-primary">Learn More</a>
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default CustomerCard;
