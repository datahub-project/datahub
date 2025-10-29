import React from 'react';
import styles from './styles.module.css';

const TutorialExercise = ({ 
  title, 
  type = 'search', 
  icon, 
  children, 
  difficulty = 'beginner',
  timeEstimate,
  platform = 'DataHub'
}) => {
  const getTypeIcon = () => {
    switch (type) {
      case 'search':
        return '🔍';
      case 'hands-on':
        return '💻';
      case 'analysis':
        return '📊';
      case 'exercise':
        return '🎯';
      default:
        return '📝';
    }
  };

  const getDifficultyColor = () => {
    switch (difficulty) {
      case 'beginner':
        return 'var(--datahub-success)';
      case 'intermediate':
        return 'var(--datahub-warning)';
      case 'advanced':
        return 'var(--datahub-error)';
      default:
        return 'var(--datahub-primary)';
    }
  };

  return (
    <div className={styles.exerciseContainer}>
      <div className={styles.exerciseHeader}>
        <div className={styles.headerLeft}>
          <div className={styles.typeIcon}>
            {icon || getTypeIcon()}
          </div>
          <div className={styles.titleSection}>
            <h4 className={styles.exerciseTitle}>{title}</h4>
            <div className={styles.metadata}>
              <span 
                className={styles.difficulty}
                style={{ backgroundColor: getDifficultyColor() }}
              >
                {difficulty}
              </span>
              {timeEstimate && (
                <span className={styles.timeEstimate}>
                  ⏱️ {timeEstimate}
                </span>
              )}
              <span className={styles.platform}>
                {platform}
              </span>
            </div>
          </div>
        </div>
      </div>
      <div className={styles.exerciseContent}>
        {children}
      </div>
    </div>
  );
};

export const SearchExercise = ({ title, searches, children, ...props }) => (
  <TutorialExercise title={title} type="search" {...props}>
    {searches && (
      <div className={styles.searchList}>
        {searches.map((search, index) => (
          <div key={index} className={styles.searchItem}>
            <div className={styles.searchQuery}>
              <code>{search.query}</code>
            </div>
            {search.description && (
              <div className={styles.searchDescription}>
                {search.description}
              </div>
            )}
            {search.expected && (
              <div className={styles.searchExpected}>
                <strong>Expected:</strong> {search.expected}
              </div>
            )}
          </div>
        ))}
      </div>
    )}
    {children}
  </TutorialExercise>
);

export const HandsOnExercise = ({ title, steps, children, ...props }) => (
  <TutorialExercise title={title} type="hands-on" {...props}>
    {steps && (
      <div className={styles.stepsList}>
        {steps.map((step, index) => (
          <div key={index} className={styles.stepItem}>
            <div className={styles.stepNumber}>{index + 1}</div>
            <div className={styles.stepContent}>
              <div className={styles.stepTitle}>{step.title}</div>
              {step.description && (
                <div className={styles.stepDescription}>{step.description}</div>
              )}
              {step.code && (
                <div className={styles.stepCode}>
                  <code>{step.code}</code>
                </div>
              )}
            </div>
          </div>
        ))}
      </div>
    )}
    {children}
  </TutorialExercise>
);

export const InteractiveDemo = ({ title, children, ...props }) => (
  <TutorialExercise title={title} type="exercise" icon="🎮" {...props}>
    <div className={styles.interactiveContent}>
      {children}
    </div>
  </TutorialExercise>
);

export default TutorialExercise;
