import React, { useEffect } from 'react';
import clsx from "clsx";
import Link from "@docusaurus/Link";
import styles from "./styles.module.scss";
import ScrollingCustomers from '../CompanyLogos';
import './hubspotFormStyles.css';

const DemoForm = ({ formId }) => {
  useEffect(() => {
    const formContainerId = `hubspotForm-${formId}`;

    const initializeHubspotForm = () => {
      if (!document.querySelector(`#${formContainerId} .hs-form`)) {
        window.hbspt.forms.create({
          region: "na1",
          portalId: "14552909",
          formId: "ed2447d6-e6f9-4771-8f77-825b114a9421",
          target: `#${formContainerId}`,
        });

        setTimeout(() => {
          const emailInput = document.querySelector(`#${formContainerId} .hs_email .input > input`);
          const firstNameInput = document.querySelector(`#${formContainerId} .hs_firstname .input > input`);
          const lastNameInput = document.querySelector(`#${formContainerId} .hs_lastname .input > input`);
          const phoneInput = document.querySelector(`#${formContainerId} .hs_phone .input > input`);
          const additionalInfoInput = document.querySelector(`#${formContainerId} .hs_additional_info .input > textarea`);

          if (emailInput) emailInput.placeholder = 'Company Email';
          if (firstNameInput) firstNameInput.placeholder = 'First Name';
          if (lastNameInput) lastNameInput.placeholder = 'Last Name';
          if (phoneInput) phoneInput.placeholder = 'Phone Number';
          if (additionalInfoInput) additionalInfoInput.placeholder = 'How can we help?';

          const selectNoEElement = document.getElementById(`number_of_employees-ed2447d6-e6f9-4771-8f77-825b114a9421`);
          if (selectNoEElement) {
            const disabledOption = selectNoEElement.querySelector('option[disabled]');
            if (disabledOption) {
              disabledOption.text = "Select Number of Employees";
              disabledOption.value = "";
            }
          }
          const selectfamiliarityElement = document.getElementById(`familiarity_with_acryl_datahub-ed2447d6-e6f9-4771-8f77-825b114a9421`);
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

    if (!window.hbspt) {
      const script = document.createElement('script');
      script.src = "//js.hsforms.net/forms/embed/v2.js";
      script.async = true;
      script.type = 'text/javascript';
      document.body.appendChild(script);

      script.onload = () => {
        initializeHubspotForm();
      };
    } else {
      initializeHubspotForm();
    }

    return () => {
      const hubspotForm = document.querySelector(`#${formContainerId} .hs-form`);
      if (hubspotForm) {
        hubspotForm.remove();
      }
    };
  }, [formId]);

  return (
    <div className={clsx(styles.formContainer)}>
      <div className={clsx(styles.formContent)}>
        <div className={clsx(styles.formHeader)}>
          <div className={clsx(styles.formTitle)}>Book a Demo</div>
          <div className={clsx(styles.formSubtitle)}>
            Schedule your personalized demo and get a trial.
          </div>
        </div>
        <div id={`hubspotForm-${formId}`} className={styles.hubspotForm}></div> {/* Use unique ID */}
      </div>
    </div>
  );
};

export default DemoForm;
