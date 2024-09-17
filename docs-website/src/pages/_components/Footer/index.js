import React from "react";
import Link from "@docusaurus/Link";
import styles from "./footer.module.scss";

const Footer = () => {
  return (
    <div className={styles.container}>
      <div className={styles.footer_wrapper}>
        <div className={styles.footer_upper}>
          <div className={styles.footer_upper_left}>
            <div className={styles.footer_product}>
              <h5>Product</h5>
              <Link to="">Acryl Datahub</Link>
              <Link to="">Acryl Observe</Link>
              <Link to="">Customer Service</Link>
            </div>
            <div className={styles.footer_community}>
              <h5>Community</h5>
              <Link to="">Blog</Link>
              <Link to="">Webinars</Link>
              <Link to="">Events</Link>
            </div>
            <div className={styles.footer_company}>
              <h5>Company</h5>
              <Link to="">About us</Link>
              <Link to="">Partners</Link>
              <Link to="">Customers</Link>
              <Link to="">Career</Link>
            </div>
          </div>
          <div className={styles.footer_upper_right}>
            <div className={styles.footer_subscribe_heading}>Subscribe</div>
            <div className={styles.footer_subscribe_input_div}>
              <input type="text" placeholder="Email address" />
              <button>→</button>
            </div>
            <div className={styles.footer_subscribe_content}>
              Hello, we are Datahub. trying to make an effort to put the right
              people for you to get the best results
            </div>
          </div>
        </div>
        <div className={styles.footer_lower}>
          <div className={styles.footer_logo}>DataHub</div>
          <div className={styles.footer_terms_privacy_div}>
            <p>Terms</p>
            <p>Privacy</p>
            <p>Cookies</p>
          </div>
          <div className={styles.footer_social_media_icon_div}>
            <div className={styles.footer_icon}>
              <img src="" alt="L" />
            </div>
            <div className={styles.footer_icon}>
              <img src="" alt="F" />
            </div>
            <div className={styles.footer_icon}>
              <img src="" alt="T" />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Footer;
