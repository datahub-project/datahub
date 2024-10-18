import React from 'react';
import styles from './hero.module.scss';
import Link from "@docusaurus/Link";

const Hero = ({ onOpenTourModal, heroContent }) => {
  const { topQuote, title, description, imgSrc } = heroContent
  return (
    <div className={styles.hero}>
      <div className={styles.hero__container}>
        <div className={styles.hero__topQuote}>
          {topQuote}
        </div>
        <div className={styles.hero__title}>
          {title}
        </div>
        <p className={styles.hero__description}>{description}</p>
        <div className={styles.hero__cta}>
          <Link className={styles.cta__primary} to="/cloud">
            DataHub Cloud →
          </Link>
          <a
            className={styles.cta__secondary}
            onClick={onOpenTourModal}
          >
            Product Tour
          </a>
        </div>
        <img
          src={imgSrc}
          alt="DataHub Platform Preview"
          className={styles.hero__img}
        />
        <div
          className={styles.hero__background}
        ></div>
      </div>
    </div>
  );
};

export default Hero;