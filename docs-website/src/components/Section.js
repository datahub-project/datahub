import React from "react";
import clsx from "clsx";
import styles from "../styles/section.module.scss";

const Section = ({ title, children, withBackground }) => (
  <section className={clsx(styles.section, withBackground && styles.withBackground)}>
    <div className="container">
      <hr />
      <div style={{ display: "flex" }}>
        <h2 className={styles.sectionTitle}>{title}</h2>
      </div>
    </div>
    {children}
  </section>
);

export default Section;
