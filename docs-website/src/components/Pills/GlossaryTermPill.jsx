import React from "react";
import styles from "./styles.module.css";

const generateTermColor = (termName) => {
  const colors = [
    "#1890ff",
    "#52c41a",
    "#faad14",
    "#f5222d",
    "#722ed1",
    "#fa541c",
    "#13c2c2",
    "#eb2f96",
    "#a0d911",
    "#fadb14",
  ];
  let hash = 0;
  for (let i = 0; i < termName.length; i++) {
    hash = (hash << 5) - hash + termName.charCodeAt(i);
  }
  return colors[Math.abs(hash) % colors.length];
};

export const GlossaryTermPill = ({ term }) => (
  <div
    className={styles.termPill}
    title={term}
    style={{ ["--pill-color"]: generateTermColor(term || "") }}
  >
    <div className={styles.termRibbon} />
    <span className={styles.termText}>{term}</span>
  </div>
);

export default GlossaryTermPill;
