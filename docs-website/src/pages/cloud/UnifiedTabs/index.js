import React, { useState } from 'react';
import { CaretUpFilled } from '@ant-design/icons';
import styles from './styles.module.scss';
import clsx from 'clsx';

const TabbedComponent = () => {
  const [activeTab, setActiveTab] = useState(0);

  const tabs = [
    {
      title: 'Discovery',
      description: 'All the search and discovery features of DataHub Core you already love, enhanced.',
      icon: "/img/assets/data-discovery.svg",
      link: "https://datahubproject.io/solutions/discovery",
      image: 'https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/demo/discovery.webm',
    },
    {
      title: 'Observability',
      description: 'Detect, resolve, and prevent data quality issues before they impact your business. Unify data health signals from all your data quality tools, including dbt tests and more.',
      icon: "/img/assets/data-ob.svg",
      link: "https://datahubproject.io/solutions/observability",
      image: 'https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/demo/observe.webm',
    },
    {
      title: 'Governance',
      description: 'Powerful Automation, Reporting and Organizational tools to help you govern effectively.',
      icon: "/img/assets/data-governance.svg",
      link: "https://datahubproject.io/solutions/governance",
      image: 'https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/saas/demo/governance.webm',
    },
  ];

  return (
    <div className={clsx(styles.tabbedComponent)}>
      <div className={clsx(styles.title)}><span>The only platform you need</span></div>
      <div className={clsx(styles.container, 'shadow--lw')}>
        <div className={clsx(styles.tabs)}>
          {tabs.map((tab, index) => (
            <React.Fragment key={index}>
              <div className={clsx(styles.tab, { [styles.activeTab]: activeTab === index })}>
                <button 
                  className={clsx(styles.tabButton, { [styles.active]: activeTab === index })}
                  onClick={() => setActiveTab(index)}
                >
                  <img className={clsx(styles.icon)} src={tab.icon} alt={`${tab.title} icon`} />
                  <div className={clsx(styles.tabTitle)}>{tab.title}</div>
                  <div className={clsx(styles.arrow, { [styles.upsideDown]: activeTab === index })}><CaretUpFilled/></div>
                </button>
                {activeTab === index && (
                  <div className={clsx(styles.dropdown)}>
                    <p>{tab.description}</p>
                    <a className={clsx(styles.learnMore)} href={tab.link} target='_blank'>Learn More →</a>
                  </div>
                )}
              </div>
              {activeTab === index && (
                <div className={clsx(styles.imageContainer, styles.mobileImageContainer)}>
                  <div className={clsx(styles.tabImage)}>
                    <video playsInline src={tabs[activeTab].image} controls={false} autoPlay muted loop />
                  </div>
                </div>
              )}
            </React.Fragment>
          ))}
        </div>
        <div className={clsx(styles.imageContainer, styles.webImageContainer)}>
          <div className={clsx(styles.tabImage)}>
            <video src={tabs[activeTab].image} controls={false} autoPlay muted loop />
          </div>
        </div>
      </div>
    </div>
  );
};

export default TabbedComponent;
