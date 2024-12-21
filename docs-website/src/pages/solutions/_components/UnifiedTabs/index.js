import React, { useState } from 'react';
import styles from './styles.module.scss';
import clsx from 'clsx';
import useBaseUrl from '@docusaurus/useBaseUrl';

const TabbedComponent = ({ unifiedTabsData }) => {
  const [activeTab, setActiveTab] = useState(0);

  return (
    <div className={clsx(styles.tabbedComponent)}>
      <div className={clsx(styles.tabsContainer)}>
        <div className={clsx(styles.tabs)}>
          {unifiedTabsData.map((tab, index) => (
            <div
              key={index}
              className={clsx(styles.tabButton, { [styles.active]: activeTab === index })}
              onClick={() => setActiveTab(index)}
            >
              <div className={clsx(styles.tabButtonText, { [styles.active]: activeTab === index })}>
                {tab.tabName}
              </div>
            </div>
          ))}
        </div>
      </div>
      <div className={clsx(styles.container)}>
        <div className={clsx(styles.tabContent)}>
          <div className={styles.tabTitle}>{unifiedTabsData[activeTab].title}</div>
          <div className={styles.tabDescription}>{unifiedTabsData[activeTab].description}</div>
        </div>

        <div className={clsx(styles.imageContainer)}>
          <div className={clsx(styles.tabImage)}>
            <img src={unifiedTabsData[activeTab].image} alt={unifiedTabsData[activeTab].imagetabName} />
          </div>
        </div>
      </div>
    </div>
  );
};

export default TabbedComponent;
