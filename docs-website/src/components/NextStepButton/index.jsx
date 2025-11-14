import React from "react";
import Link from "@docusaurus/Link";
import styles from "./styles.module.css";

const NextStepButton = ({
  to,
  children,
  tutorialId,
  currentStep,
  variant = "primary",
  icon = "→",
}) => {
  const handleClick = () => {
    if (tutorialId && currentStep !== undefined) {
      const storageKey = `datahub-tutorial-${tutorialId}`;
      const savedProgress = localStorage.getItem(storageKey);
      let completedSteps = new Set();

      if (savedProgress) {
        try {
          completedSteps = new Set(JSON.parse(savedProgress));
        } catch (e) {
          console.warn("Failed to parse tutorial progress:", e);
        }
      }

      // Mark current step as completed
      completedSteps.add(`step-${currentStep}`);
      localStorage.setItem(storageKey, JSON.stringify([...completedSteps]));
    }
  };

  return (
    <Link
      to={to}
      className={`${styles.nextStepButton} ${styles[variant]}`}
      onClick={handleClick}
    >
      <span className={styles.content}>
        {children}
        <span className={styles.icon}>{icon}</span>
      </span>
    </Link>
  );
};

export default NextStepButton;
