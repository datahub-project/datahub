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
                <Typography.Title level={3}>Welcome to DataHub! 👋</Typography.Title>
                <Typography.Paragraph style={{ lineHeight: '22px' }}>
                    <strong>DataHub</strong> helps you discover and organize the important data within your
                    organization. You can:
                </Typography.Paragraph>
                <Typography.Paragraph style={{ lineHeight: '24px' }}>
                    <ul>
                        <li>
                            Quickly <strong>search</strong> for Datasets, Dashboards, Data Pipelines, and more
                        </li>
                        <li>
                            View and understand the full <strong>end-to-end Lineage</strong> of how data is created,
                            transformed, and consumed
                        </li>
                        <li>
                            Gain <strong>insights</strong> about how others within your organization are using data
                        </li>
                        <li>
                            Define <strong>ownership</strong> and capture <strong>knowledge</strong> to empower others
                        </li>
                    </ul>
                    <p>Let&apos;s get started! 🚀</p>
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
        title: 'Ingest Data',
        content: (
            <Typography.Paragraph>
                Start integrating your data sources immediately by navigating to the <strong>Ingestion</strong> page.
            </Typography.Paragraph>
        ),
    },
    {
        id: HOME_PAGE_DOMAINS_ID,
        selector: `#${HOME_PAGE_DOMAINS_ID}`,
        title: 'Explore by Domain',
        content: (
            <Typography.Paragraph>
                Here are your organization&apos;s <strong>Domains</strong>. Domains are collections of data assets -
                such as Tables, Dashboards, and ML Models - that make it easy to discover information relevant to a
                particular part of your organization.
            </Typography.Paragraph>
        ),
    },
    {
        id: HOME_PAGE_PLATFORMS_ID,
        selector: `#${HOME_PAGE_PLATFORMS_ID}`,
        title: 'Explore by Platform',
        content: (
            <Typography.Paragraph>
                Here are your organization&apos;s <strong>Data Platforms</strong>. Data Platforms represent specific
                third-party Data Systems or Tools. Examples include Data Warehouses like <strong>Snowflake</strong>,
                Orchestrators like
                <strong>Airflow</strong>, and Dashboarding tools like <strong>Looker</strong>.
            </Typography.Paragraph>
        ),
    },
    {
        id: HOME_PAGE_MOST_POPULAR_ID,
        selector: `#${HOME_PAGE_MOST_POPULAR_ID}`,
        title: 'Explore Most Popular',
        content: "Here you'll find the assets that are viewed most frequently within your organization.",
    },
    {
        id: HOME_PAGE_SEARCH_BAR_ID,
        selector: `#${HOME_PAGE_SEARCH_BAR_ID}`,
        title: 'Find your Data 🔍',
        content: (
            <Typography.Paragraph>
                <p>
                    This is the <strong>Search Bar</strong>. It will serve as your launch point for discovering and
                    collaborating around the data most important to you.
                </p>
                <p>
                    Not sure where to start? Click on <strong>Explore All</strong>!
                </p>
            </Typography.Paragraph>
        ),
    },
];
