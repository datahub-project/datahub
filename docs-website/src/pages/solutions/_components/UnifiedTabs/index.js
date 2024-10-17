import React, { useState } from 'react';
import { CaretUpFilled } from '@ant-design/icons';
import styles from './styles.module.scss';
import clsx from 'clsx';

const TabbedComponent = ({unifiedTabsData}) => {
  const [activeTab, setActiveTab] = useState(0); 

  return (
    <div className={clsx(styles.tabbedComponent)}>
      <div className={clsx(styles.container, 'shadow--lw')}>
        <div className={clsx(styles.tabs)}>
          {unifiedTabsData.map((tab, index) => (
            <React.Fragment key={index}>
              <div className={clsx(styles.tab, { [styles.activeTab]: activeTab === index })}>
                <button 
                  className={clsx(styles.tabButton, { [styles.active]: activeTab === index })}
                  onClick={() => setActiveTab(index)}
                >
                  <div className={clsx(styles.tabTitle)}>{tab.tabName}</div>
                  <div className={clsx(styles.arrow, { [styles.upsideDown]: activeTab === index })}><CaretUpFilled/></div>
                </button>
                {activeTab === index && (
                  <div className={clsx(styles.dropdown)}>
                    <div className={styles.tabTitle}>{tab.title}</div>
                    <div className={styles.tabDescription}>{tab.description}</div>
                    <a className={clsx(styles.learnMore)} href={tab.link} target='_blank'>Learn More →</a>
                  </div>
                )}
              </div>
              {activeTab === index && (
                <div className={clsx(styles.imageContainer, styles.mobileImageContainer)}>
                  <div className={clsx(styles.tabImage)}>
                    <video playsInline src={tab.image} controls={false} autoPlay muted loop />
                  </div>
                </div>
              )}
            </React.Fragment>
          ))}
        </div>
        <div className={clsx(styles.imageContainer, styles.webImageContainer)}>
          <div className={clsx(styles.tabImage)}>
            <video src={unifiedTabsData[activeTab].image} controls={false} autoPlay muted loop />
          </div>
        </div>
      </div>
    </div>
  );
};

export default TabbedComponent;
