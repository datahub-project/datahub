import React from "react";
import clsx from "clsx";
import styles from "./quickstartcard.module.scss";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";


const QuickstartCard = ({ icon, title, to, color, fontColor }) => {
  return (
    <div className="col col--6">
      <Link to={to} className={clsx("card", styles.feature)} style={{ background: color, color: fontColor}}>
        <div className={styles.card_content}>
            <img src={useBaseUrl(`/img/${icon}.svg`)} />
            <div style={{ margin: "auto 0"}}>
              <div className={styles.card_title}>
                {title}&nbsp;→
              </div>
            </div>
        </div>
      </Link>
    </div>
  );
};


export default QuickstartCard;
