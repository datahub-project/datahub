import React from 'react';
import { Image, Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';
import { ANTD_GRAY } from '../../entity/shared/constants';

export const GLOBAL_WELCOME_TO_DATAHUB_ID = 'global-welcome-to-datahub';
export const HOME_PAGE_INGESTION_ID = 'home-page-ingestion';
export const HOME_PAGE_DOMAINS_ID = 'home-page-domains';
export const HOME_PAGE_PLATFORMS_ID = 'home-page-platforms';
export const HOME_PAGE_MOST_POPULAR_ID = 'home-page-most-popular';
export const HOME_PAGE_SEARCH_BAR_ID = 'home-page-search-bar';

export const HomePageOnboardingConfig: OnboardingStep[] = [
    {
        id: GLOBAL_WELCOME_TO_DATAHUB_ID,
        content: (
            <div>
                <Image
                    preview={false}
                    height={184}
                    width={500}
                    style={{ marginLeft: '50px' }}
                    src="https://datahubproject.io/assets/ideal-img/datahub-flow-diagram-light.5ce651b.1600.png"
                />
                <Typography.Title level={3}>欢迎使用 DataHub! 👋</Typography.Title>
                <Typography.Paragraph style={{ lineHeight: '22px' }}>
                    <strong>DataHub</strong> 可以高效的帮准您进行数据发现。您可以:
                </Typography.Paragraph>
                <Typography.Paragraph style={{ lineHeight: '24px' }}>
                    <ul>
                        <li>
                            使用 <strong>search</strong> 快速的查找数据资产，比如Datasets, Dashboards, Data Pipelines等;
                        </li>
                        <li>
                            使用 <mark>可视化的数据血缘</mark> 帮助您理解数据是如何生成，如何处理，以及如何使用的;
                        </li>
                        <li>
                            通过 <strong>分析</strong> 来知晓您组织内的其它人是如何使用这些数据及数据资产;
                        </li>
                        <li>
                            定义 <strong>所有者</strong> 并分享 <strong>这些知识</strong> 赋能团队中的每个人！
                        </li>
                    </ul>
                    <p>让我们开始吧! 🚀</p>
                    <div
                        style={{
                            backgroundColor: ANTD_GRAY[4],
                            opacity: '0.7',
                            borderRadius: '4px',
                            height: '40px',
                            display: 'flex',
                            alignItems: 'center',
                        }}
                    >
                        <span style={{ paddingLeft: '5px' }}>💡</span>
                        <span style={{ paddingLeft: '10px' }}>
                            Press <strong> Cmd + Ctrl + T</strong> to open up this tutorial at any time.
                        </span>
                    </div>
                </Typography.Paragraph>
            </div>
        ),
        style: { minWidth: '650px' },
    },
    {
        id: HOME_PAGE_INGESTION_ID,
        selector: `#${HOME_PAGE_INGESTION_ID}`,
        title: '元数据集成',
        content: (
            <Typography.Paragraph>
                点击 <strong>元数据集成</strong> . 开始您的元数据集成之旅吧！
            </Typography.Paragraph>
        ),
    },
    {
        id: HOME_PAGE_DOMAINS_ID,
        selector: `#${HOME_PAGE_DOMAINS_ID}`,
        title: '根据 Domain 浏览',
        content: (
            <Typography.Paragraph>
                这里是您组织内的 <strong>Domains</strong>. Domains 是您组织内数据资产的集合，
                比如 Tables, Dashboards, 以及 ML Models。 让您更简便的探索特定领域内的数据及数据集.
            </Typography.Paragraph>
        ),
    },
    {
        id: HOME_PAGE_PLATFORMS_ID,
        selector: `#${HOME_PAGE_PLATFORMS_ID}`,
        title: '根据 Platform 浏览',
        content: (
            <Typography.Paragraph>
                这里是您组织内的 <strong>Data Platforms</strong>. Data Platforms 代表着特定的第三方系统或着工具.
                比如，类似<strong>Snowflake</strong> 的数据仓库, 像<strong>Airflow</strong>一样的调度工具 ，
                以及仪表盘工具 <strong>Looker</strong>.
            </Typography.Paragraph>
        ),
    },
    {
        id: HOME_PAGE_MOST_POPULAR_ID,
        selector: `#${HOME_PAGE_MOST_POPULAR_ID}`,
        title: '最受欢迎的数据资产',
        content: '这里是在您组织内浏览最多的数据资产.',
    },
    {
        id: HOME_PAGE_SEARCH_BAR_ID,
        selector: `#${HOME_PAGE_SEARCH_BAR_ID}`,
        title: '找到您关心的数据 🔍',
        content: (
            <Typography.Paragraph>
                <p>
                    通过 <strong>Search Bar</strong> 开启您的 <strong>数据发现</strong> 之旅 .
                </p>
                <p>
                    不确定从哪里开始? 点击 <strong>Explore All</strong>!
                </p>
            </Typography.Paragraph>
        ),
    },
];
