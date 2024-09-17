import React, { useRef } from "react";
import styles from "./case-study.module.scss";
import Link from "@docusaurus/Link";

const CaseStudy = () => {
  const cardRowRef = useRef(null);

  const handleScrollRight = () => {
    const slider = cardRowRef.current;
    slider.scrollBy({
      left: 300,
      behavior: "smooth",
    });
  };

  const handleScrollLeft = () => {
    const slider = cardRowRef.current;
    slider.scrollBy({
      left: -300,
      behavior: "smooth",
    });
  };

  return (
    <div className={styles.container}>
      <div className={styles.case_study}>
        <div className={styles.case_study_heading}>
          <div>See how Industry leaders use the Datahub</div>
          <p>Across finance, healthcare, e-commerce and more.</p>
        </div>

        <button
          className={`${styles.arrow} ${styles.left_arrow}`}
          onClick={handleScrollLeft}
        >
          ←
        </button>

        <div className={styles.card_row} ref={cardRowRef}>
          <div className={styles.card}>
            <span className={styles.card_tag}>ENTERTAINMENT</span>
            <img
              src="https://i0.wp.com/picjumbo.com/wp-content/uploads/logo-netflix-free-photo.jpg"
              alt="Netflix Case Study"
            />
            <div className={styles.card_heading_div}>
              <div className={styles.card_heading}>
                <span>Netflix Case Study</span>
              </div>
              <div className={styles.card_para}>
                Lorem ipsum dolor sit amet consectetur adipisicing elit. Modi,
                minima!
              </div>
            </div>
          </div>
          <div className={styles.card}>
            <span className={styles.card_tag}>FINANCE</span>
            <img
              src="https://cdn.pmnewsnigeria.com/wp-content/uploads/2021/09/bitcoin-255x175.jpg"
              alt=""
            />
            <div className={styles.card_heading_div}>
              <div className={styles.card_heading}>
                <span>Netflix Case Study</span>
              </div>
              <div className={styles.card_para}>
                Lorem ipsum dolor sit amet consectetur adipisicing elit. Modi,
                minima!
              </div>
            </div>
          </div>
          <div className={styles.card}>
            <span className={styles.card_tag}>MEDICAL</span>
            <img
              src="https://static.vecteezy.com/system/resources/thumbnails/002/414/650/small/hand-with-protective-gloves-holding-a-blood-samples-for-covid-test-free-photo.jpg"
              alt=""
            />
            <div className={styles.card_heading_div}>
              <div className={styles.card_heading}>
                <span>Netflix Case Study</span>
              </div>
              <div className={styles.card_para}>
                Lorem ipsum dolor sit amet consectetur adipisicing elit. Modi,
                minima!
              </div>
            </div>
          </div>{" "}
          <div className={styles.card}>
            <span className={styles.card_tag}>MEDICAL</span>
            <img
              src="https://static.vecteezy.com/system/resources/thumbnails/002/414/650/small/hand-with-protective-gloves-holding-a-blood-samples-for-covid-test-free-photo.jpg"
              alt=""
            />
            <div className={styles.card_heading_div}>
              <div className={styles.card_heading}>
                <span>Netflix Case Study</span>
              </div>
              <div className={styles.card_para}>
                Lorem ipsum dolor sit amet consectetur adipisicing elit. Modi,
                minima!
              </div>
            </div>
          </div>
          <div className={styles.card}>
            <span className={styles.card_tag}>E-LEARNING</span>
            <img
              src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRySxTniNHamPd5PBUC-c6Hqsx-On1nM6eol71UJFWWd41qnrR8MXIkB0nuqT2Lnuahg7c&usqp=CAU"
              alt=""
            />
            <div className={styles.card_heading_div}>
              <div className={styles.card_heading}>
                <span>Netflix Case Study</span>
              </div>
              <div className={styles.card_para}>
                Lorem ipsum dolor sit amet consectetur adipisicing elit. Modi,
                minima!
              </div>
            </div>
          </div>
        </div>

        {/* Right Arrow */}
        <button
          className={`${styles.arrow} ${styles.right_arrow}`}
          onClick={handleScrollRight}
        >
          →
        </button>

        <Link className={styles.bottom_line} to="/adoption-stories">
          See all adoption stories →
        </Link>
      </div>
    </div>
  );
};

export default CaseStudy;
