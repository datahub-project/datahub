import React from "react";
import styles from "./styles.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";
import clsx from "clsx";
import { Link } from "@docusaurus/router";

const Trials = ({ onOpenTourModal, trialsContent }) => {
  const { title, trialsCardItems } = trialsContent;

  return (
    <div className={styles.container}>
      <div className={styles.trial}>
        <div className={styles.trial_left}>
          <div className={styles.left_content}>
            <p className={styles.trial_title}>{title}</p>
            <div className={styles.btn_div}>
              <Link to="/cloud">Book a Demo</Link>
              <a onClick={onOpenTourModal}>Product Tour</a>
            </div>
            <Link className={styles.start_arrow} to="/docs">
              Get started with Core →
            </Link>
          </div>
        </div>
        <div className={styles.trial_right}>
          <div className={styles.right_content}>
            <div className={styles.gradientTop} />
            <div className={styles.gradientBottom} />

            {trialsCardItems.map((item, index) => (
              <div
                className={clsx(
                  styles.card_wrapper,
                  index < 2 ? styles.right_l : styles.right_r
                )}
                key={index}
              >
                <div
                  className={clsx(
                    styles.card,
                    index === 0 ? styles.soc :
                    index === 1 ? styles.cost :
                    index === 2 ? styles.enterprise : styles.link
                  )}
                >
                  <img width={60} height={60} src={useBaseUrl(item.imgSrc)} alt={item.title} />
                  <p className={styles.card_text} dangerouslySetInnerHTML={{ __html: item.title }} />
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};

export default Trials;
