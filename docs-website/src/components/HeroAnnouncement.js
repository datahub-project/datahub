import React from "react";
import clsx from "clsx";
import Link from "@docusaurus/Link";
import styles from "../styles/hero-announcement.module.scss";

const HeroAnnouncement = ({ message, linkUrl, linkText }) => (
  <div className={clsx("hero__alert alert alert--primary", styles.hero__alert)}>
    <span>{message}</span>
    {linkUrl && (
      <Link className="button button--primary button--md" href={linkUrl} target="_blank">
        {linkText}
      </Link>
    )}
  </div>
);

export default HeroAnnouncement;
