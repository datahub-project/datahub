import React, { useEffect } from 'react';
import clsx from "clsx";
import Link from "@docusaurus/Link";
import styles from "./styles.module.scss";
import ScrollingCustomers from '../CompanyLogos';
import './hubspotFormStyles.css';

const Hero = () => {
  useEffect(() => {
    const script = document.createElement('script');
    script.src = "//js.hsforms.net/forms/embed/v2.js";
    script.async = true;
    script.type = 'text/javascript';
    document.body.appendChild(script);

    script.onload = () => {
      if (window.hbspt) {
        window.hbspt.forms.create({
          region: "na1",
          portalId: "14552909",
          formId: "ed2447d6-e6f9-4771-8f77-825b114a9421",
          target: '#hubspotForm',
        });

        // Modify placeholders after the form has loaded
        setTimeout(() => {
          const emailInput = document.querySelector('#hubspotForm .hs_email .input > input');
          const firstNameInput = document.querySelector('#hubspotForm .hs_firstname .input > input');
          const lastNameInput = document.querySelector('#hubspotForm .hs_lastname .input > input');
          const phoneInput = document.querySelector('#hubspotForm .hs_phone .input > input');
          const additionalInfoInput = document.querySelector('#hubspotForm .hs_additional_info .input > textarea');

          if (emailInput) emailInput.placeholder = 'Company Email';
          if (firstNameInput) firstNameInput.placeholder = 'First Name';
          if (lastNameInput) lastNameInput.placeholder = 'Last Name';
          if (phoneInput) phoneInput.placeholder = 'Phone Number';
          if (additionalInfoInput) additionalInfoInput.placeholder = 'How can we help?';

          const selectNoEElement = document.getElementById("number_of_employees-ed2447d6-e6f9-4771-8f77-825b114a9421");
          if (selectNoEElement) {
            const disabledOption = selectNoEElement.querySelector('option[disabled]');
            if (disabledOption) {
              disabledOption.text = "Select Number of Employees";
              disabledOption.value = "";
            }
          }
          const selectfamiliarityElement = document.getElementById("familiarity_with_acryl_datahub-ed2447d6-e6f9-4771-8f77-825b114a9421");
          if (selectfamiliarityElement) {
            const disabledOption = selectfamiliarityElement.querySelector('option[disabled]');
            if (disabledOption) {
              disabledOption.text = "How familiar are you with DataHub?";
              disabledOption.value = "";
            }
          }

        }, 1000); // Delay to ensure the form is fully loaded

        window.hero = new RevenueHero({ routerId: '982' });
        window.hero.schedule('hsForm_ed2447d6-e6f9-4771-8f77-825b114a9421');
      }
    };

    return () => {
      document.body.removeChild(script);
    };
  }, []);

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
                    with Data Observability and Data Governance built-in.
                  </div>
                </div>
                <Link className={clsx(styles.button, styles.bookButton, "button button--primary button--lg")} to="https://www.acryldata.io/datahub-sign-up?utm_source=datahub&utm_medium=referral&utm_campaign=acryl_signup">
                  Book Demo
                </Link>
                <Link className={clsx(styles.button, styles.productTourButton, "button button--secondary button--lg")} to="https://www.acryldata.io/tour">
                  Live Product Tour →
                </Link>
                <ScrollingCustomers />
            </div>
            <div className={clsx(styles.col, "col col--5")}>
              <div className={clsx(styles.formContainer)}>
                <div className={clsx(styles.formContent)}>
                  <div className={clsx(styles.formHeader)}>
                    <div className={clsx(styles.formTitle)}>Book a free Demo</div>
                    <div className={clsx(styles.formSubtitle)}>
                      Schedule a personalized demo and get a free a trial.
                    </div>
                  </div>
                  <div id="hubspotForm" className={styles.hubspotForm}></div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </header>
  );
};

export default Hero;
