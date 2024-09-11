import React from "react";
import styles from "./socialmedia.module.scss";
import "swiper/css";
import "swiper/css/pagination";
import {
  ArrowUpOutlined,
  LinkedinOutlined,
  MediumWorkmarkOutlined,
  YoutubeOutlined,
} from "@ant-design/icons";


const SocialMedia = ({}) => {
  return (
    <div className={styles.container}>
      <div className={styles.mainBox}>
        <div className={styles.upperBox}>
          <div>
            Built <strong>for</strong> Data Practitioners, <strong>by</strong>{" "}
            Data Practitioners
          </div>
        </div>
        <div style={{ height: "40vh" }}></div>
        <div className={styles.socialMediaSection}>
          <div className={styles.socialSubText}>
            Born at LinkedIn, driven by Acryl <br />
            and 500+ community contributors.
          </div>
          <div className={styles.socialStats}>
            <div
              className={styles.statItem}
              style={{
                background: "#A2A6B4",
                border: "1px solid #ffffff",
                position: "relative",
              }}
            >
              <div className={styles.styledIcon}>
                <YoutubeOutlined
                  width={38}
                  height={38}
                  className={styles.mediaIcons}
                />
              </div>
              <div className={styles.statName}>
                YouTube
                <div className={styles.followerCount}> 2.9k subscribers</div>
              </div>
              <ArrowUpOutlined
                width={13}
                height={13}
                rotate={45}
                className={styles.visitPageIcon}
              />
            </div>
            <div className={styles.statItem}>
              <div className={styles.styledIcon}>
                <LinkedinOutlined
                  width={38}
                  height={38}
                  className={styles.mediaIcons}
                />
              </div>
              <div className={styles.statName}>
                LinkedIn
                <div className={styles.followerCount}>3.5k followers</div>
              </div>
            </div>
            <div className={styles.statItem}>
              <div className={styles.styledIcon}>
                <MediumWorkmarkOutlined
                  width={38}
                  height={38}
                  className={styles.mediaIcons}
                />
              </div>
              <div className={styles.statName}>
                Newsletter
                <div className={styles.followerCount}>900 subscribers</div>
              </div>
            </div>
            <div className={styles.statItem}>
              <div className={styles.styledIcon}>
                <MediumWorkmarkOutlined
                  width={38}
                  height={38}
                  className={styles.mediaIcons}
                />
              </div>
              <div className={styles.statName}>
                Medium
                <div className={styles.followerCount}>1k subscribers</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SocialMedia;
