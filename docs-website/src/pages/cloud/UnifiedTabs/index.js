import React, { useState } from 'react';
import { CaretUpFilled } from '@ant-design/icons';
import styles from './styles.module.scss';
import clsx from 'clsx';

const TabbedComponent = () => {
  const [activeTab, setActiveTab] = useState(0);

  const tabs = [
    {
      title: 'Data Discovery',
      description: 'All the search and discovery features of DataHub Core you already love, enhanced.',
      icon: "/img/assets/data-discovery.svg",
      link: "https://www.acryldata.io/acryl-datahub",
      image: '/path/to/image1.png',
    },
    {
      title: 'Data Observability',
      description: 'Detect, resolve, and prevent data quality issues before they impact your business. Unify data health signals from all your data quality tools, including dbt tests and more.',
      icon: "/img/assets/data-ob.svg",
      link: "https://www.acryldata.io/observe",
      image: '/path/to/image2.png',
    },
    {
      title: 'Data Governance',
      description: 'Powerful Automations, Reporting and Organizational tools to help you govern effectively.',
      icon: "/img/assets/data-governance.svg",
      link: "https://www.acryldata.io/acryl-datahub#governance",
      image: '/path/to/image3.png',
    },
  ];

  return (
    <div className={clsx(styles.tabbedComponent)}>
      <div className={clsx(styles.title)}><span>One platform to rule them all</span></div>
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
                  <div style={{ backgroundImage: `url(${tabs[activeTab].image})` }} alt={tabs[activeTab].title} className={clsx(styles.tabImage)} />
                </div>
              )}
            </React.Fragment>
          ))}
        </div>
        <div className={clsx(styles.imageContainer, styles.webImageContainer)}>
          <div style={{ backgroundImage: `url(${tabs[activeTab].image})` }} alt={tabs[activeTab].title} className={clsx(styles.tabImage)} />
        </div>
      </div>
    </div>
  );
};

export default TabbedComponent;
