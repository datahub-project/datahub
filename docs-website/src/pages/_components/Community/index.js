import React, { useState, useRef, useEffect } from "react";
import styles from "./community.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";

const TARGET_COUNT = 12219;
const INCREMENT = 1;

const Community = () => {
  const currentCountRef = useRef(TARGET_COUNT - 50);
  const [count, setCount] = useState(currentCountRef.current);
  const hasAnimatedRef = useRef(false);
  const counterRef = useRef(null);

  const animateNumber = () => {
    const makeTimeout = () => {
      const distance = TARGET_COUNT - currentCountRef.current;
      const isSlowCount = distance < 10;
      const isMediumCount = distance < 20;
      setTimeout(() => {
        if (currentCountRef.current < TARGET_COUNT) {
          currentCountRef.current += INCREMENT;
          setCount(currentCountRef.current);
          makeTimeout();
        }
      }, isSlowCount ? Math.random() * 6000 : (isMediumCount ? 150 : 50)); 
    }
    makeTimeout();
  };
  const handleScroll = () => {
    if (hasAnimatedRef.current) return;
    if (!counterRef.current) return;

    const { top } = counterRef.current.getBoundingClientRect();
    const windowHeight = window.innerHeight;

    if (top <= windowHeight) {
      hasAnimatedRef.current = true;
      animateNumber();
    }
  };

  const formattedCount = count.toLocaleString();

  useEffect(() => {
    window.addEventListener('scroll', handleScroll);
    return () => {
      window.removeEventListener('scroll', handleScroll);
    }
  }, [])

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
              Q&A.&nbsp;&nbsp;Office Hours.&nbsp;&nbsp;Think Tanks.&nbsp;&nbsp;Job Postings.
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
                      {[1, 2, 3, 4, 5, 6].map((item, index) => (
                        <div className={styles.slide} key={index} style={{ backgroundImage: `url(${useBaseUrl(`/img/slack/slack-community-user-${item}.png`)})` }}>
                          <div className={styles.slideCrown} style={{ backgroundImage: `url(${useBaseUrl('/img/slack/Crown.png')})` }} />
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
