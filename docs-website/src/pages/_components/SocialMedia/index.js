import React, { useEffect, useRef, useState } from "react";
import useBaseUrl from "@docusaurus/useBaseUrl";
import Link from "@docusaurus/Link";
import styles from "./socialmedia.module.scss";
import "swiper/css";
import "swiper/css/pagination";
import {
  ArrowUpOutlined,
  LinkedinOutlined,
  ReadOutlined,
  MediumWorkmarkOutlined,
  YoutubeOutlined,
} from "@ant-design/icons";
import { Carousel } from "antd";


const VideoItem = ({ isActive, src }) => {
  const ref = useRef();

  useEffect(() => {
    if (isActive) {
      ref.current?.play();
    } else {
      ref.current?.pause();
    }
  }, [isActive])

  return <video
    ref={ref}
    muted
    loop
    // autoPlay
    preload="auto"
    controls={false}
    src={src}
    className={styles.video}
  />

}
function useWindowDimensions() {

  const hasWindow = typeof window !== 'undefined';

  function getWindowDimensions() {
    const width = hasWindow ? window.innerWidth : null;
    const height = hasWindow ? window.innerHeight : null;
    return {
      width,
      height,
    };
  }

  const [windowDimensions, setWindowDimensions] = useState(getWindowDimensions());

  useEffect(() => {
    function handleResize() {
      setWindowDimensions(getWindowDimensions());
    }

    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  return windowDimensions;
}


const SocialMedia = ({}) => {

  const sliderVideos = [
    {
        videoUrl: useBaseUrl("/img/home-social-media/kathleen.webm"),
        title: "Insider Secrets: Building Bulletproof Analytics Teams w/ Kathleen Maley",
        link: 'https://www.youtube.com/watch?v=ER6OHT6wTbc',
        // description: "VP Data Analytics, Experian",
        date: "Jun 13, 2024",
        viewerCount: "100+",
    },
    // {
    //     videoUrl: useBaseUrl("/img/home-social-media/visa_speaker.webm"),
    //     link: 'https://www.youtube.com/watch?v=B6CplqnIkFw',
    //     title: "The VISA Team's vision for Logical Datasets",
    //     // description: "VP Data Analytics, Experian",
    //     date: "Apr 18, 2024",
    //     viewerCount: "900+",
    // },
    {
      videoUrl: useBaseUrl("/img/home-social-media/linkedin.webm"),
      link: 'https://www.youtube.com/watch?v=3alQ9e6Lf2Y',
      title: 'AI and Data: The LinkedIn Story w/ Kapil Surlaker',
      date: "Apr 11, 2024",
      viewerCount: "100+",
    },
    {
        videoUrl: useBaseUrl("/img/home-social-media/hashi.mp4"),
        title: "Why Sean Rice of HashiCorp loves the time to value of Acryl Data.",
        link: 'https://www.youtube.com/watch?v=a3Rgb9QYbUk',
        // description: "VP Data Analytics, Experian",
        date: "July 2024",
        viewerCount: "300+",
    },
  ];
  
  const [activeSliderIndex, setActiveSliderIndex] = useState(0);
  const { width } = useWindowDimensions();
  const isMobileView = width <= 800;

  const onChange = (currentSlide, nextSlideRaw) => {
    setActiveSliderIndex((nextSlideRaw + 1) % sliderVideos.length)
  };
      
  return (
    <div className={styles.container}>
        <div className={styles.containerBG} style={{ backgroundImage: `url(${useBaseUrl('/img/home-social-media/section-background.svg')})` }} />

        {/* <div className={styles.upperBox}>
          <div>
            Built <strong>for</strong> Data Practitioners,<br/><strong>by</strong>{" "}
            Data Practitioners.
          </div>
        </div> */}
      <div className={styles.mainBox}>
        <div className={styles.socialMediaSection}>
          <div className={styles.socialSubText}>
            Built <strong>for</strong> Data Practitioners,<br/><strong>by</strong>{" "}
            Data Practitioners.
            {/* Born at LinkedIn, driven by Acryl <br />
            and 500+ community contributors. */}
          </div>
          <div className={styles.socialStats}>
            <Link className={styles.statItem} to="https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w">
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
            </Link>
            <Link className={styles.statItem} to="https://www.linkedin.com/company/acryl-data/">
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
            </Link>
            <Link className={styles.statItem} to="https://www.linkedin.com/newsletters/datahub-newsletter-7129989188422160384/">
              <div className={styles.styledIcon}>
                <ReadOutlined
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
            </Link>
            <Link className={styles.statItem}  to="https://blog.datahubproject.io/">
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
            </Link>
          </div>
        </div>
      </div>
      {isMobileView ? null :
        <div className={styles.carousalContainer}>
          <div className={styles.carousalWrapper}>
            <Carousel slidesToShow={3} initialSlide={2} dots={false} dotPosition="left" infinite autoplay speed={300} autoplaySpeed={3000}
              beforeChange={onChange}
            >
              {sliderVideos.map((video, idx) => (
                <Link className={styles.videoContainer} to={video.link} key={video.link}>
                  <VideoItem
                    isActive={activeSliderIndex === idx}
                    src={video.videoUrl}
                  />
                  <div className={styles.videoItemFooter}>
                    <div className={styles.videoTitle}>{video.title}</div>
                    <div className={styles.videoDetails}>
                        {/* <div className={styles.videoDescription}>
                        {video.description}
                        </div>
                        <div className={styles.divider}/> */}
                        <div className={styles.videoDescription}>{video.date}</div>
                        <div className={styles.divider}/>
                        <div className={styles.videoDescription}>{video.viewerCount} views</div>
                    </div>
                  </div>
                </Link>
              ))}
            </Carousel>
          </div>
        </div>
      }
    </div>
  );
};

export default SocialMedia;