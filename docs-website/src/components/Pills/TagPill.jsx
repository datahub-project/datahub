import React from "react";
import styles from "./styles.module.css";

const generateTagColor = (tagName) => {
  let hash = 0;
  for (let i = 0; i < tagName.length; i++) {
    const char = tagName.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash = hash & hash;
  }
  const hue = Math.abs(hash) % 360;
  return `hsl(${hue}, 70%, 45%)`;
};

export const TagPill = ({ tag }) => (
  <div className={styles.tagPill} title={tag}>
    <div
      className={styles.tagColorDot}
      style={{ backgroundColor: generateTagColor(tag || "") }}
    />
    <span className={styles.tagText}>{tag}</span>
  </div>
);

export default TagPill;
