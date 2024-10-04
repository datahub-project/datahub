import React from "react";
import styles from "./tiles.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";
import clsx from "clsx";

const Tiles = ({ tilesContent }) => {
  const { title, tileItems } = tilesContent;

  return (
    <div className={styles.container}>
      <div className={styles.ecosystem_section}>
        <div className={styles.ecosystem_section_content}>
          <div className={styles.ecosystem_section_upper_content}>
            <div className={styles.ecosystem_section_heading}>{title}</div>
          </div>
          <div className={styles.ecosystem_section_lower_content}>
            <div className={styles.itemWrappers}>
              {tileItems.map((item, index) => (
                <div
                  key={index}
                  className={clsx("row", styles.itemWrapper, {
                    [styles.alternate]: index % 2 !== 0, // Alternate placing for diagonal effect on mobile
                  })}
                >
                  <div className={clsx(styles.item, "col col--5")}>
                    <div className={styles.item_content}>
                      <div className={styles.item__title}>{item.title}</div>
                      <div className={styles.item__subtitle}>{item.subtitle}</div>
                    </div>
                  </div>
                  <div
                    className={clsx(styles.diagramItem, "col col--5")}
                    style={{
                      backgroundImage: `url(${useBaseUrl(item.imgSrc)})`,
                    }}
                  ></div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Tiles;
