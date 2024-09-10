import React from "react";
import styles from "./socialmedia.module.scss";
import { Swiper } from "swiper/react";
import "swiper/css";
import "swiper/css/pagination";

const sliderVideos = [
  {
    name: "Demo Video 1",
    imageUrl: "./demo.mp4",
  },
];

const SocialMedia = ({}) => {
  return (
    <div className={styles.container}>
      <div>
        <div className={styles.upperSection}>
          <div>
            Built <strong>for</strong> Data Practitioners, <strong>by</strong>{" "}
            Data Practitioners
          </div>
        </div>
        <div className={styles.rightBox}>
          <div className={styles.socialSubText}>
            Born at LinkedIn, driven by Acryl <br />
            and 500+ community contributors.
          </div>
          <div className={styles.socialStats}>
            <div className={styles.statItem}>
              <div className={styles.statName}>YouTube</div>
              <div> 2.9k subscribers </div>
            </div>
            <div className={styles.statItem}>
              <div className={styles.statName}>LinkedIn</div>
              <div>3.5k followers</div>
            </div>
            <div className={styles.statItem}>
              <div className={styles.statName}>Newsletter</div>
              <div>900 subscribers</div>
            </div>
            <div className={styles.statItem}>
              <div className={styles.statName}>Medium</div>
              <div>1k subscribers</div>
            </div>
          </div>
          <Swiper slidesPerView={1} spaceBetween={10} slidesPerGroup={2} />
        </div>
      </div>
    </div>
  );
};

export default SocialMedia;
