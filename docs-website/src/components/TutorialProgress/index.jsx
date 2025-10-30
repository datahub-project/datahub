import React, { useState, useEffect } from "react";
import { useHistory, useLocation } from "@docusaurus/router";
import styles from "./styles.module.css";

const TutorialProgress = ({
  tutorialId,
  steps,
  currentStep,
  compact = false,
}) => {
  const [completedSteps, setCompletedSteps] = useState(new Set());
  const [isMinimized, setIsMinimized] = useState(false);
  const [isScrolled, setIsScrolled] = useState(false);

  // Handle both old and new formats
  const actualTutorialId = tutorialId || "tutorial";
  const actualCurrentStep =
    typeof currentStep === "string" ? currentStep : `step-${currentStep}`;
  const storageKey = `datahub-tutorial-${actualTutorialId}`;

  // Load progress from localStorage on component mount
  useEffect(() => {
    const savedProgress = localStorage.getItem(storageKey);
    if (savedProgress) {
      try {
        const parsed = JSON.parse(savedProgress);
        setCompletedSteps(new Set(parsed));
      } catch (e) {
        console.warn("Failed to parse tutorial progress:", e);
      }
    }
  }, [storageKey]);

  // Save progress to localStorage whenever completedSteps changes
  useEffect(() => {
    localStorage.setItem(storageKey, JSON.stringify([...completedSteps]));
  }, [completedSteps, storageKey]);

  const toggleStep = (stepId) => {
    setCompletedSteps((prev) => {
      const newSet = new Set(prev);
      if (newSet.has(stepId)) {
        newSet.delete(stepId);
      } else {
        newSet.add(stepId);
        // Auto-mark previous steps as completed
        const stepIndex = parseInt(stepId.split("-")[1]);
        for (let i = 0; i < stepIndex; i++) {
          newSet.add(`step-${i}`);
        }
      }
      return newSet;
    });
  };

  const resetProgress = () => {
    setCompletedSteps(new Set());
    localStorage.removeItem(storageKey);
  };

  // Auto-mark current step as completed when user navigates
  useEffect(() => {
    if (currentStep !== undefined) {
      setCompletedSteps((prev) => {
        const newSet = new Set(prev);
        newSet.add(actualCurrentStep);
        return newSet;
      });
    }
  }, [actualCurrentStep]);

  // Handle scroll behavior for auto-minimizing
  useEffect(() => {
    const handleScroll = () => {
      const scrollTop =
        window.pageYOffset || document.documentElement.scrollTop;
      setIsScrolled(scrollTop > 100); // Auto-minimize after scrolling 100px
    };

    window.addEventListener("scroll", handleScroll);
    return () => window.removeEventListener("scroll", handleScroll);
  }, []);

  const toggleMinimized = () => {
    setIsMinimized(!isMinimized);
  };

  const completionPercentage = Math.round(
    (completedSteps.size / steps.length) * 100,
  );

  if (compact) {
    return (
      <div className={`${styles.tutorialProgress} ${styles.compact}`}>
        <div className={styles.compactHeader}>
          <span className={styles.compactTitle}>
            📋 Progress: {completedSteps.size}/{steps.length}
          </span>
          <div className={styles.compactBar}>
            <div
              className={styles.progressFill}
              style={{ width: `${completionPercentage}%` }}
            />
          </div>
        </div>
      </div>
    );
  }

  // Determine if we should show minimized version
  const shouldShowMinimized = isMinimized || isScrolled;

  if (shouldShowMinimized) {
    return (
      <div
        className={`${styles.tutorialProgress} ${styles.minimized} ${isScrolled ? styles.scrolled : ""}`}
      >
        <div className={styles.minimizedHeader} onClick={toggleMinimized}>
          <div className={styles.minimizedContent}>
            <span className={styles.minimizedTitle}>
              📋 {completedSteps.size}/{steps.length} completed (
              {completionPercentage}%)
            </span>
            <div className={styles.minimizedBar}>
              <div
                className={styles.progressFill}
                style={{ width: `${completionPercentage}%` }}
              />
            </div>
          </div>
          <button
            className={styles.expandButton}
            title="Expand progress details"
          >
            ⬇️
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className={styles.tutorialProgress}>
      <div className={styles.header}>
        <div className={styles.headerContent}>
          <h4>📋 Tutorial Progress</h4>
          <button
            className={styles.minimizeButton}
            onClick={toggleMinimized}
            title="Minimize progress tracker"
          >
            ⬆️
          </button>
        </div>
        <div className={styles.progressBar}>
          <div
            className={styles.progressFill}
            style={{ width: `${completionPercentage}%` }}
          />
          <span className={styles.progressText}>
            {completedSteps.size} of {steps.length} completed (
            {completionPercentage}%)
          </span>
        </div>
      </div>

      <div className={styles.stepsList}>
        {steps.map((step, index) => {
          // Handle both old format (step-${index}) and new format (step.id)
          const stepId = step.id || `step-${index}`;
          const isCompleted = completedSteps.has(stepId);
          const isCurrent = actualCurrentStep === stepId;

          return (
            <div
              key={stepId}
              className={`${styles.step} ${isCurrent ? styles.current : ""}`}
            >
              <label className={styles.stepLabel}>
                <input
                  type="checkbox"
                  checked={isCompleted}
                  onChange={() => toggleStep(stepId)}
                  className={styles.checkbox}
                />
                <span className={styles.checkmark}>
                  {isCompleted ? "✅" : "⬜"}
                </span>
                <span className={styles.stepText}>
                  <strong>{step.title || step.label}</strong>
                  {step.time && (
                    <span className={styles.time}>({step.time})</span>
                  )}
                  {isCurrent && (
                    <span className={styles.currentBadge}>← You are here</span>
                  )}
                </span>
              </label>
              {step.description && (
                <div className={styles.stepDescription}>{step.description}</div>
              )}
            </div>
          );
        })}
      </div>

      <div className={styles.actions}>
        <button
          onClick={resetProgress}
          className={styles.resetButton}
          title="Reset all progress for this tutorial"
        >
          🔄 Reset Progress
        </button>
        {completedSteps.size === steps.length && (
          <div className={styles.completionMessage}>
            🎉 <strong>Tutorial Complete!</strong> Great job finishing all
            steps!
          </div>
        )}
      </div>
    </div>
  );
};

export default TutorialProgress;
