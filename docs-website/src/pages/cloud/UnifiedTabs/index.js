import React, { useState } from 'react';
import { UpOutlined } from '@ant-design/icons';
import styles from './styles.module.scss';
import clsx from 'clsx';

const TabbedComponent = () => {
  const [activeTab, setActiveTab] = useState(0);

  const tabs = [
    {
      title: 'Data Discovery',
      description: 'Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet.',
      icon: "/img/assets/data-discovery.svg",
      link: "/",
      image: '/path/to/image1.png',
    },
    {
      title: 'Data Observability',
      description: 'Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet.',
      icon: "/img/assets/data-ob.svg",
      link: "/",
      image: '/path/to/image2.png',
    },
    {
      title: 'Data Governance',
      description: 'Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet.',
      icon: "/img/assets/data-governance.svg",
      link: "/",
      image: '/path/to/image3.png',
    },
  ];

  return (
    <div className={clsx(styles.tabbedComponent)}>
      <div className={clsx(styles.title)}>One unified experience</div>
      <div className={clsx(styles.container, 'shadow--tl')}>
        <div className={clsx(styles.tabs)}>
          {tabs.map((tab, index) => (
            <div key={index} className={clsx(styles.tab, { [styles.activeTab]: activeTab === index })}>
              <button 
                className={clsx(styles.tabButton, { [styles.active]: activeTab === index })}
                onClick={() => setActiveTab(index)}
              >
                <img className={clsx(styles.icon)} src={tab.icon} alt={`${tab.title} icon`} />
                <div className={clsx(styles.tabTitle)}>{tab.title}</div>
                <div className={clsx(styles.arrow, { [styles.upsideDown]: activeTab === index })}><UpOutlined/></div>
              </button>
              {activeTab === index && (
                <div className={clsx(styles.dropdown)}>
                  <p>{tab.description}</p>
                  <a className={clsx(styles.learnMore)} href={tab.link}>Learn More →</a>
                </div>
              )}
            </div>
          ))}
        </div>
        <div className={clsx(styles.imageContainer)}>
          <img src={tabs[activeTab].image} alt={tabs[activeTab].title} className={clsx(styles.tabImage)}/>
        </div>
      </div>
    </div>
  );
};

export default TabbedComponent;
