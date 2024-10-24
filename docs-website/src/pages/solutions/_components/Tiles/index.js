import React from "react";
import styles from "./tiles.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";
import clsx from "clsx";

const Tiles = ({ tilesContent }) => {
  const { title, theme, tileItems } = tilesContent;

  const sectionThemeClass = theme === "dark" ? styles.darkSection : styles.lightSection;
  const itemThemeClass = theme === "dark" ? styles.darkItem : styles.lightItem;
  const diagramItemThemeClass = theme === "dark" ? styles.darkDiagramItem : styles.lightDiagramItem;

  return (
    <div className={clsx(sectionThemeClass)}>
      <div className={styles.ecosystem_section}>
        <div className={styles.ecosystem_section_content}>
          <div className={styles.ecosystem_section_lower_content}>
            <div className={styles.itemWrappers}>
              <div className={styles.ecosystem_section_upper_content}>
                <div className={styles.ecosystem_section_heading} dangerouslySetInnerHTML={{ __html: title }}></div>
              </div>
              {tileItems.map((item, index) => (
                <div
                  key={index}
                  className={clsx(styles.itemWrapper, {
                    [styles.alternate]: index % 2 === 0,
                  })}
                >
                  {index % 2 !== 0 ? (
                    <>
                      <div
                        className={clsx(
                          styles.diagramItem,
                          diagramItemThemeClass,
                      
                        )}
                      >
                        <img className={styles.diagramItem__img} src={useBaseUrl(item.imgSrc)} alt={item.title} />
                      </div>
                      <div
                        className={clsx(styles.item, styles.evenItem, itemThemeClass)}
                      >
                        <div className={styles.item_content}>
                          <div className={styles.item__title}>{item.title}</div>
                          <div className={styles.item__subtitle}>{item.subtitle}</div>
                        </div>
                      </div>
                    </>
                  ) : (
                    <>
                      <div
                        className={clsx(styles.item, styles.oddItem, itemThemeClass)}
                      >
                        <div className={clsx(styles.item_content)}>
                          <div className={styles.item__title}>{item.title}</div>
                          <div className={styles.item__subtitle}>{item.subtitle}</div>
                        </div>
                      </div>
                      <div
                        className={clsx(
                          styles.diagramItem,
                          diagramItemThemeClass,
                      
                        )}
                      >
                        <img className={styles.diagramItem__img} src={useBaseUrl(item.imgSrc)} alt={item.title} />
                      </div>
                    </>
                  )}
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
