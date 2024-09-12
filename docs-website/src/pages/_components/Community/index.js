import React, { useState, useRef } from "react";
import styles from "./community.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Item1 from "../../../../static/img/slack/slack-community-user-1.png";
import Item2 from "../../../../static/img/slack/slack-community-user-2.png";
import Item3 from "../../../../static/img/slack/slack-community-user-3.png";
import Item4 from "../../../../static/img/slack/slack-community-user-4.png";
import Item5 from "../../../../static/img/slack/slack-community-user-5.png";
import Item6 from "../../../../static/img/slack/slack-community-user-6.png";

const Community = () => {
  const [count, setCount] = useState(10235);
  const [hasAnimated, setHasAnimated] = useState(false);
  const counterRef = useRef(null);

  const handleScroll = () => {
    if (!hasAnimated && counterRef.current) {
      const { top } = counterRef.current.getBoundingClientRect();
      const windowHeight = window.innerHeight;

      if (top <= windowHeight) {
        setHasAnimated(true);
        animateNumber();
      }
    }
  };

  const animateNumber = () => {
    let currentCount = 10335;
    const targetCount = 10500;
    const increment = 1;

    const interval = setInterval(() => {
      if (currentCount < targetCount) {
        currentCount += increment;
        setCount(currentCount);
      } else {
        clearInterval(interval);
      }
    }, 50);
  };

  const formattedCount = count.toLocaleString();

  window.onscroll = handleScroll;

  return (
    <div className={styles.container}>
      <div className={styles.community_section}>
        <div className={styles.community_section_left}>
          <div className={styles.community_section_left_content}>
            <div className={styles.community_section_heading}>
              A community that's
              <div>
                <img
                  width={30}
                  height={30}
                  src={useBaseUrl("/img/logos/companies/slack.svg")}
                  alt="Slack"
                />
                <span ref={counterRef} className={styles.numberContainer}>
                  <span
                    className={styles.numberChange}
                    style={{ color: "rgba(255,255,255,1)" }}
                  >
                    {formattedCount}
                  </span>
                </span>{" "}
                strong.
              </div>
            </div>
            <p>
              Q&A.&emsp;Office Hours.&emsp; Monthly Town Hall.&emsp; Job
              Postings.
            </p>
            <a href="/slack">Join Slack</a>
          </div>
        </div>
        <div className={styles.community_section_right}>
          <div className={styles.community_section_heading}>
            <div className={styles.community_section_subText}>
              <div>
                With{" "}
                <img
                  width={20}
                  height={20}
                  src={useBaseUrl("/img/github.png")}
                  alt="GitHub"
                />{" "}
                500+ contributors <br /> world-wide.
              </div>
              <a href="https://github.com/datahub-project/datahub">Open GitHub</a>
            </div>
            <div className={styles.carouselContainer}>
              <div className={styles.slider}>
                <div className={styles.slide_track}>
                  {/* Duplicate the slides */}
                  {[...Array(2)].map((_, i) => (
                    <React.Fragment key={i}>
                      {[Item1, Item2, Item3, Item4, Item5, Item6].map((item, index) => (
                        <div className={styles.slide} key={index}>
                          <img src={item} alt={`Item ${index}`} />
                        </div>
                      ))}
                    </React.Fragment>
                  ))}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Community;
