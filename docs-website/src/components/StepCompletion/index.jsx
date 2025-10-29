import React, { useState, useEffect } from 'react';
import styles from './styles.module.css';

const StepCompletion = ({ stepId, children, completionText = "✅ Completed!" }) => {
  const [isCompleted, setIsCompleted] = useState(false);
  const storageKey = `datahub-step-${stepId}`;

  // Load completion status from localStorage
  useEffect(() => {
    const saved = localStorage.getItem(storageKey);
    if (saved === 'true') {
      setIsCompleted(true);
    }
  }, [storageKey]);

  // Save completion status to localStorage
  useEffect(() => {
    localStorage.setItem(storageKey, isCompleted.toString());
  }, [isCompleted, storageKey]);

  const toggleCompletion = () => {
    setIsCompleted(!isCompleted);
  };

  return (
    <div className={`${styles.stepCompletion} ${isCompleted ? styles.completed : ''}`}>
      <div className={styles.content}>
        {children}
      </div>
      <div className={styles.completionControl}>
        <label className={styles.completionLabel}>
          <input
            type="checkbox"
            checked={isCompleted}
            onChange={toggleCompletion}
            className={styles.checkbox}
          />
          <span className={styles.checkmark}>
            {isCompleted ? '✅' : '⬜'}
          </span>
          <span className={styles.completionText}>
            {isCompleted ? completionText : 'Mark as complete'}
          </span>
        </label>
      </div>
    </div>
  );
};

export default StepCompletion;
