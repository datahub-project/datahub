import React, { useState } from 'react';
import styles from './styles.module.scss';
import clsx from 'clsx';

const TabbedComponent = () => {
  const [activeTab, setActiveTab] = useState(0);

  const tabs = [
    {
      title: 'Deploy with enterprise-grade security',
      description: 'Acryl Observe deploys and runs in your own VPC, offering pre-configured support for advanced networking features—like AWS PrivateLink, or Private Google Access—to facilitate secure, private connectivity to cloud services. Plus, both Observe and Acryl Cloud are certified to meet rigorous compliance and security standards, like SOC 2.',
      icon: "/img/solutions/lock.png",
    },
    {
      title: 'Scale from Zero to Infinity',
      description: 'Acryl Observe is built for any scale. Leveraging the power of Acryl Cloud, Observe can scale to support data warehouses with petabytes of data in tens of thousands of tables—and tens of billions of rows. And because it’s a fully managed SaaS offering, it’s also ideal for small organizations still building out their data ecosystems.',
      icon: "/img/solutions/rocket-launch.png",
    },
    {
      title: 'Reduce tool clutter and operational burden',
      description: 'Simplify your stack. Avoid duplication across tools by unifying data discovery, data governance, and data quality into one central tool. Skip spending countless engineering hours maintaining inaccessible, code-first data quality frameworks',
      icon: "/img/solutions/communities.png",
    },
    {
      title: 'Reduce the risk of vendor lock-in',
      description: 'Get the benefits of open source in a fully managed, limitlessly scalable SaaS offering. Acryl Observe and Acryl Cloud are built on top of the DataHub Project, proven open-source technology with an active, thriving community of contributors and users. Customers get 100% compatibility with open-source DataHub, plus regular updates and improvements, source code transparency, community-based support, proven security, and protection against vendor lock-in.',
      icon: "/img/solutions/water-lock.png",
    }
  ];

  return (
    <div className={clsx(styles.tabbedComponent)}>
      <div className={clsx(styles.container)}>
        <div className={clsx(styles.leftSection)}>
          <div className={clsx(styles.title)}>
            Secure. Scalable.<br/>Simple. <span className={clsx(styles.titleBlue)}> Open.</span>
          </div>
          <div className={clsx(styles.tabs)}>
            {tabs.map((tab, index) => (
              <React.Fragment key={index}>
                <div className={clsx(styles.tab, { [styles.activeTab]: activeTab === index })}>
                  <button 
                    className={clsx(styles.tabButton, { [styles.active]: activeTab === index })}
                    onClick={() => setActiveTab(index)}
                  >
                    <img className={clsx(styles.icon)} src={tab.icon} alt={`${tab.title} icon`} />
                    <div className={clsx(styles.tabTitle, { [styles.activeTitle]: activeTab === index })}>
                      {tab.title}
                    </div>
                  </button>
                  {activeTab === index && (
                    <div className={clsx(styles.dropdown)}>
                      {tab.description}
                    </div>
                  )}
                </div>
              </React.Fragment>
            ))}
          </div>
        </div>
        <div className={clsx(styles.imageContainer)}>
          <div className={clsx(styles.tabImage)} style={{ backgroundImage: `url(/img/solutions/sliding-tab-bg.png)`}} />
        </div>
      </div>
    </div>
  );
};

export default TabbedComponent;
