import React from "react";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";
import styles from "./guidelist.module.scss";

const ListItem = ({ icon, platformIcon, title, to }) => {
  return (
    <div className="col col--3">
      <Link to={useBaseUrl(to)} className={styles.listItem}>
        {icon}
        {platformIcon && <img src={useBaseUrl(`/img/logos/platforms/${platformIcon}.svg`)} />}
        <strong>{title}</strong>
      </Link>
    </div>
  );
};

const GuideList = ({ title, content, seeMoreLink }) =>
  content?.length > 0 ? (
    <div style={{ padding: "2vh 0" }}>
      <div className="container">
        <h2 style={{ fontWeight: "normal" }}>{title}</h2>
        <div className="row row--no-gutters">
          {content.map((props, idx) => (
            <ListItem key={idx} {...props} />
          ))}
        </div>
        {seeMoreLink?.to && (
          <div className="margin-top--md">
            <Link className="button button--link padding-left--none" to={seeMoreLink?.to}>
              {seeMoreLink?.label} →
            </Link>
          </div>
        )}
      </div>
    </div>
  ) : null;

export default GuideList;
