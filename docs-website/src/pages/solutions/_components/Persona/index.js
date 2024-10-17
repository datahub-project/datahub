import React from "react";
import styles from "./styles.module.scss";
import clsx from "clsx";
import Link from '@docusaurus/Link'

const Persona = ({ personaContent }) => {
  const { title, personas } = personaContent;
  return (
    <div className={styles.container}>
      <div className={styles.personas}>
        <div className={styles.persona_heading}>
          {title}
        </div>

        <div className={clsx(styles.persona_row)}>
          <div className={styles.persona_row_wrapper}>
            {personas.map((persona) => (
              <div className={clsx(styles.persona)}>
                <div className={clsx(styles.persona_img)}>
                  <img src={persona.imgSrc} alt={persona.alt} />
                </div>
                <div className={clsx(styles.persona_desc_div)}>
                  <div className={styles.persona_desc}>{persona.desc}</div>
                  <Link className={styles.persona_link} to={persona.link}>More...</Link>
                </div>
              </div>
            ))}
          </div>
        </div>

        <div className={styles.persona_bg_line}></div>
      </div>
    </div>
  );
};


export default Persona;