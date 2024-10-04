import React, { useState, useRef, useEffect } from "react";
import styles from "./integrations.module.scss";
import useBaseUrl from "@docusaurus/useBaseUrl";

const Integrations = () => {
  const hasAnimatedRef = useRef(false);
  const counterRef = useRef(null);
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

  useEffect(() => {
    window.addEventListener('scroll', handleScroll);
    return () => {
      window.removeEventListener('scroll', handleScroll);
    }
  }, [])

  return (
    <div className={styles.container}>
      <div className={styles.section_header}>
          Integrates with your data stack
      </div>
      <div className={styles.community_section}>
            <div className={styles.carouselContainer}>
              <div className={styles.slider}>
                <div className={styles.slide_track}>
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
  );
};

export default Integrations;
