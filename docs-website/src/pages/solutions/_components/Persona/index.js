import React from "react";
import styles from "./styles.module.scss";
import clsx from "clsx";

const Persona = ({ personaContent }) => {
  const { title, personas } = personaContent;

  return (
    <div className={styles.container}>
      <div className={styles.personas}>
        <div className={styles.persona_heading}>{title}</div>

        <div className={clsx(styles.persona_row)}>
          <div className={styles.persona_row_wrapper}>
            <div className={styles.persona_bg_line}></div>
            {personas.map((persona, index) => (
              <div key={index} className={clsx(styles.persona)}>
                <div className={clsx(styles.persona_img)}>
                  <img src={persona.imgSrc} alt={persona.alt} />
                </div>
                <div className={styles.features}>
                  <FeatureItem text={persona.feature1} />
                  <FeatureItem text={persona.feature2} />
                  <FeatureItem text={persona.feature3} />
                  <FeatureItem text={persona.feature4} />
                </div>
              </div>
            ))}
          </div>
        </div>
        <div className={styles.persona_row_mobile}>
          <img src='/img/solutions/personas-mobile.png' alt="Persona" />
        </div>
      </div>
      <div className={styles.card_gradient} />
    </div>
  );
};

const FeatureItem = ({ text }) => (
  <div className={styles.featureItem}>
    {text}
  </div>
);

export default Persona;
