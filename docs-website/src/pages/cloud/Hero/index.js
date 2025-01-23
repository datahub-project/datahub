import React, { useState } from 'react';
import clsx from "clsx";
import Link from "@docusaurus/Link";
import styles from "./styles.module.scss";
import ScrollingCustomers from '../CompanyLogos';
import DemoForm from '../DemoForm';
import DemoFormModal from '../DemoFormModal';

const Hero = () => {
  const [isModalOpen, setIsModalOpen] = useState(false);

  const handleOpenModal = () => setIsModalOpen(true);
  const handleCloseModal = () => setIsModalOpen(false);

  return (
    <header className={clsx("hero", styles.hero)}>
      <div className="container">
        <div className="hero__content">
          <div className="row row__padded">
            <div className={clsx(styles.hero__cta, styles.col, "col col--7")}>
              <h1 className={clsx("hero__title", styles.hero__title)}>DataHub Cloud</h1>
              <div className={clsx("hero__subtitle", styles.hero__subtitle)}>
                Experience the premium version of DataHub
                <div style={{ fontWeight: "500" }}>
                  with Observability and Governance built-in.
                </div>
              </div>

              <button
                className={clsx(styles.button, styles.bookButton, "button button--primary button--lg")}
                onClick={handleOpenModal}
              >
                Book Demo
              </button>

              <Link
                className={clsx(styles.button, styles.productTourButton, "button button--secondary button--lg")}
                to="https://www.acryldata.io/tour"
              >
                Live Product Tour →
              </Link>
              <ScrollingCustomers />
            </div>
            <div className={clsx(styles.col, styles.hideOnMobile, "col col--5")}>
              <DemoForm formId="heroForm" />
            </div>
          </div>
        </div>
      </div>

      {isModalOpen && (
        <DemoFormModal formId="heroFormMobile" handleCloseModal={handleCloseModal} />
      )}
    </header>
  );
};

export default Hero;
