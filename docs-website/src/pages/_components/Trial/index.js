import React from "react";
import styles from "./trial.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";

const Trial = ({onOpenTourModal}) => {
  return (
    <div className={styles.container}>
      <div className={styles.trial}>
        <div className={styles.trial_left}>
          <div className={styles.left_content}>
            <span className="">Discover. Observe. Govern.</span>
            <p className="">
              Get started with<br/><b>DataHub</b> cloud today.
            </p>
            <div className={styles.btn_div}>
              <Link to="/cloud">Book a Demo</Link>
              <a
                // to="https://www.acryldata.io/tour"
                onClick={onOpenTourModal}
              >Product Tour</a>
            </div>
            <Link className={styles.start_arrow} to="/docs">Get started with Open Source →</Link>
          </div>
        </div>
        <div className={styles.trial_right}>
          <div className={styles.right_content}>
            <div className={styles.gradientTop} />
            <div className={styles.gradientBottom} />
            <div className={styles.right_l}>
              <div className={styles.soc}>
                <img
                  width={60}
                  height={60}
                  src={useBaseUrl("/img/lock-soc.svg")}
                />
                Secure<br/> out of box with <br/>SOC2 compliance.
              </div>
              <div className={styles.cost}>
                <img
                  width={60}
                  height={60}
                  src={useBaseUrl("/img/dollar.svg")}
                />
                Reduce tool clutter,
                <br /> operational burden <br /> and costs.
              </div>
            </div>
            <div className={styles.right_r}>
              <div className={styles.enterprise}>
                <img
                  width={60}
                  height={60}
                  src={useBaseUrl("/img/building.svg")}
                />
                Built for <br /> Enterprise Scale.
              </div>
              <div className={styles.link}>
                <img width={60} height={75} src={useBaseUrl("/img/link.svg")} />
                Centralize all context
                <br /> in one place.
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Trial;
