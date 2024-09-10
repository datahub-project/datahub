import React from "react";
import styles from "./case-study.module.scss";

const CaseStudy = () => {
  return (
    <div className={styles.container}>
      {/* Section-1 */}
      <div className={styles.case_study}>
        <div className={styles.case_study_heading}>
          <div>See how Industry leaders use the Datahub</div>
          <p>Across finance, healthcare, e-commerce and more.</p>
        </div>

        <div className={styles.card_row}>
          <div className={styles.card}>
            <span className={styles.card_tag}>ENTERTAINMENT</span>
            <img
              src="https://i0.wp.com/picjumbo.com/wp-content/uploads/logo-netflix-free-photo.jpg"
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

        <div className={styles.bottom_line}>
          See all adoption stories<span>→</span>
        </div>
      </div>
    </div>
  );
};

export default CaseStudy;
