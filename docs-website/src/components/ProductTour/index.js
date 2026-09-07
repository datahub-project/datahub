import React from "react";
import styles from "./producttour.module.scss";

// Interactive product tours are hosted at tours.datahub.com. This embeds one
// live, in-page. The tours are built for desktop, so on narrow screens we swap
// the embed for a link card (CSS-only, SSR-safe).
const BASE = "https://tours.datahub.com";

export default function ProductTour({ name, title, ratio = 62.5 }) {
  const url = `${BASE}/${name}/`;
  const label =
    title || name.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
  return (
    <figure className={styles.tour}>
      <div className={styles.frame} style={{ paddingTop: `${ratio}%` }}>
        <iframe
          className={styles.iframe}
          src={url}
          title={`${label} — interactive product tour`}
          loading="lazy"
          allow="fullscreen"
          allowFullScreen
        />
      </div>
      <a
        className={styles.fallback}
        href={url}
        target="_blank"
        rel="noopener noreferrer"
      >
        <span className={styles.play}>▶</span>
        <span>
          Take the interactive <b>{label}</b> tour{" "}
          <span className={styles.hint}>(best viewed on desktop)</span>
        </span>
      </a>
      <figcaption className={styles.caption}>
        Interactive tour ·{" "}
        <a href={url} target="_blank" rel="noopener noreferrer">
          open full screen ↗
        </a>
      </figcaption>
    </figure>
  );
}
