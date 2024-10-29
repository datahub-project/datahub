import React from "react";
import styles from "./styles.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";
import clsx from "clsx";
import Link from "@docusaurus/Link";

const Trials = ({ onOpenTourModal, trialsContent }) => {
  const { title, trialsCardItems } = trialsContent;

  return (
    <div className={styles.container}>
      <div className={styles.trial}>
        <div className={styles.trial_left}>
          <div className={styles.left_content}>
            <span className="">Discover. Observe. Govern.</span>
            <p className="">{title}</p>
            <div className={styles.btn_div}>
              <Link to="/cloud">Book a Demo</Link>
              <a onClick={onOpenTourModal}>Product Tour</a>
            </div>
            <Link className={styles.start_arrow} to="/docs">
              Get started with Open Source →
            </Link>
          </div>
        </div>
        <div className={styles.trial_right}>
          <div className={styles.right_content}>
            <div className={styles.gradientTop} />
            <div className={styles.gradientBottom} />
            <div className={styles.right_l}>
              {trialsCardItems.slice(0, 2).map((item, index) => (
                <div
                  key={index}
                  className={clsx(styles[item.className])} 
                >
                  <img
                    width={60}
                    height={60}
                    src={useBaseUrl(item.imgSrc)}
                    alt={item.title}
                  />
                  {item.title.split("\n").map((line, idx) => (
                    <React.Fragment key={idx}>
                      {line}
                      <br />
                    </React.Fragment>
                  ))}
                </div>
              ))}
            </div>
            <div className={styles.right_r}>
              {trialsCardItems.slice(2).map((item, index) => (
                <div
                  key={index}
                  className={clsx(styles[item.className])} 
                >
                  <img
                    width={60}
                    height={60}
                    src={useBaseUrl(item.imgSrc)}
                    alt={item.title}
                  />
                  {item.title.split("\n").map((line, idx) => (
                    <React.Fragment key={idx}>
                      {line}
                      <br />
                    </React.Fragment>
                  ))}
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Trials;
