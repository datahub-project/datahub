import React from "react";
import clsx from "clsx";
import styles from "./quickstartcard.module.scss";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";


const QuickstartCard = ({ icon, title, to, color, fontColor }) => {
  return (
    <div className="col col--6">
      <Link to={to} className={clsx("card", styles.feature)} style={{ background: color, color: fontColor}}>
        <img src={useBaseUrl(`/img/${icon}.svg`)} />
        <div style={{ margin: "auto 0"}}>
          <strong>{title}&nbsp;→</strong>
        </div>
      </Link>
    </div>
  );
};


export default QuickstartCard;
