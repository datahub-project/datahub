import React, { useState } from "react";
import styles from "./socialmedia.module.scss";
import "swiper/css";
import "swiper/css/pagination";
import {
  ArrowUpOutlined,
  LinkedinOutlined,
  MediumWorkmarkOutlined,
  YoutubeOutlined,
} from "@ant-design/icons";
import video from "./demo.mp4";
import { Carousel } from "antd";

const sliderVideos = [
  {
    videoUrl: video,
    title: "Kathleen Maley from Experian talks about data leadership",
    description: "VP Data Analytics, Experian",
    date: "12 Aug 2024",
    subscriberCount: "12k",
  },
  {
    videoUrl: video,
    title: "Kathleen Maley from Experian talks about data leadership",
    description: "VP Data Analytics, Experian",
    date: "12 Aug 2024",
    subscriberCount: "12k",
  },
  {
    videoUrl: video,
    title: "Kathleen Maley from Experian talks about data leadership",
    description: "VP Data Analytics, Experian",
    date: "12 Aug 2024",
    subscriberCount: "12k",
  },
];

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
        <div className={styles.socialMediaSection}>
          <div className={styles.socialSubText}>
            Born at LinkedIn, driven by Acryl <br />
            and 500+ community contributors.
          </div>
          <div className={styles.socialStats}>
            <div className={styles.statItem}>
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
              <ArrowUpOutlined
                width={13}
                height={13}
                rotate={45}
                className={styles.visitPageIcon}
              />
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
              <ArrowUpOutlined
                width={13}
                height={13}
                rotate={45}
                className={styles.visitPageIcon}
              />
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
              <ArrowUpOutlined
                width={13}
                height={13}
                rotate={45}
                className={styles.visitPageIcon}
              />
            </div>
          </div>
        </div>
      </div>
      <div className={styles.carousalContainer}>
        <div className={styles.carousalWrapper}>
        <Carousel slidesToShow={3} dotPosition="left" infinite autoplay>
          {sliderVideos.map((video, idx) => (
            <div className={styles.videoContainer} key={idx}>
              <video
                autoPlay
                width="100%"
                height="auto"
                src={video.videoUrl}
                className={styles.video}
              />
              <div className={styles.videoTitle}>{video.title}</div>
              <div className={styles.videoDetails}>
                <div className={styles.videoDescription}>
                  {video.description}
                </div>
                <div className={styles.divider}>.</div>
                <div className={styles.videoDescription}>{video.date}</div>
                <div className={styles.divider}>.</div>
                <div>{video.subscriberCount}</div>
              </div>
            </div>
          ))}
        </Carousel>
        </div>
      </div>
    </div>
  );
};

export default SocialMedia;
