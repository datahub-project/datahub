import React from 'react';
import { Image, Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';
import dataHubFlowDiagram from '../../../images/datahub-flow-diagram-light.png';
import { ANTD_GRAY } from '../../entityV2/shared/constants';

export const V2_SEARCH_BAR_ID = 'v2-search-bar';
export const V2_HOME_PAGE_MOST_POPULAR_ID = 'v2-home-page-most-popular';
export const V2_SEARCH_BAR_VIEWS = 'v2-search-bar-views';
export const V2_HOME_PAGE_DISCOVER_ID = 'v2-home-page-discover';
export const V2_HOME_PAGE_ANNOUNCEMENTS_ID = 'v2-home-page-announcements';
export const V2_HOME_PAGE_PERSONAL_SIDEBAR_ID = 'v2-home-page-personal-sidebar';
export const V2_HOME_PAGE_PENDING_TASKS_ID = 'v2-home-page-pending-tasks';
export const V2_HOME_PAGE_LEARNING_CENTER_ID = 'v2-home-page-learning-center';
export const GLOBAL_WELCOME_TO_ACRYL_ID = 'global-welcome-to-acryl';

const HomePageOnboardingConfig: OnboardingStep[] = [
    {
        id: GLOBAL_WELCOME_TO_ACRYL_ID,
        content: (
            <div>
                <div
                    style={{
                        width: '540px', // Adjusted width to be wider than the image
                        borderRadius: '10px',
                        display: 'flex',
                        justifyContent: 'center',
                        alignItems: 'center',
                        margin: '0 auto 20px auto',
                    }}
                >
                    <Image preview={false} height={184} width={500} src={dataHubFlowDiagram} />
                </div>
                <Typography.Title level={3}>Welcome to DataHub! </Typography.Title>
                <Typography.Paragraph style={{ lineHeight: '22px' }}>
                    <strong>DataHub</strong> helps you discover, govern and ensure high quality for the important data
                    within your organization. You can:
                </Typography.Paragraph>
                <Typography.Paragraph style={{ lineHeight: '24px' }}>
                    <ul>
                        <li>
                            Quickly <strong>search</strong> for Tables, Dashboards, Data Pipelines, and more
                        </li>
                        <li>
                            Understand <strong>quality</strong> and trustworthiness of data using operational and social
                            signals
                        </li>
                        <li>
                            View and understand the full <strong>end-to-end lineage</strong> of how data is created,
                            transformed, and consumed
                        </li>
                        <li>
                            Gain <strong>insights</strong> about how others within your organization are using data
                        </li>
                        <li>
                            Define <strong>ownership</strong> and capture <strong>knowledge</strong> to empower others
                        </li>
                        <li>
                            Create and manage <strong>central governance standards</strong> for data assets to drive
                            accountability and trust
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
                            Press <strong>Cmd + Ctrl + T</strong> to open up this tutorial at any time.
                        </span>
                    </div>
                </Typography.Paragraph>
            </div>
        ),
        style: { minWidth: '650px' },
    },
    {
        id: V2_SEARCH_BAR_ID,
        selector: `#${V2_SEARCH_BAR_ID}`,
        title: 'Find your Data 🔍',
        content: (
            <Typography.Paragraph>
                <p>
                    This is the <strong>Search Bar</strong>. It will serve as your launch point for discovering and
                    collaborating around the data most important to you.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: V2_SEARCH_BAR_VIEWS,
        selector: `#${V2_SEARCH_BAR_VIEWS}`,
        title: 'Only the stuff you need 📷',
        content: (
            <Typography.Paragraph>
                <p>
                    Views help you focus on the assets that you care about. You can switch between views using the
                    dropdown. Your admin will configure the views that make sense for your organization. You can
                    customize and create your own views as well.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: V2_HOME_PAGE_MOST_POPULAR_ID,
        selector: `#${V2_HOME_PAGE_MOST_POPULAR_ID}`,
        title: 'Explore Most Popular',
        content: "Here you'll find the assets that are viewed most frequently within your organization.",
    },
    {
        id: V2_HOME_PAGE_DISCOVER_ID,
        selector: `#${V2_HOME_PAGE_DISCOVER_ID}`,
        title: 'Discover 🔍',
        content: (
            <Typography.Paragraph>
                <p>
                    The <strong> Discover</strong> section serves as your exploration center for discovering new areas
                    of your data estate. You can explore Domains, Platforms, and more.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: V2_HOME_PAGE_ANNOUNCEMENTS_ID,
        selector: `#${V2_HOME_PAGE_ANNOUNCEMENTS_ID}`,
        title: 'Announcements 📣',
        content: (
            <Typography.Paragraph>
                <p>
                    The <strong> Announcements</strong> tab contains important updates and information from your
                    organization. Be sure to check it out frequently!
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: V2_HOME_PAGE_PERSONAL_SIDEBAR_ID,
        selector: `#${V2_HOME_PAGE_PERSONAL_SIDEBAR_ID}`,
        title: 'Your Personal Sidebar 📌',
        content: (
            <Typography.Paragraph>
                <p>
                    This is your <strong> Personal Sidebar</strong>. It contains links to assets you own, groups you are
                    in, your subscriptions and more.
                </p>
            </Typography.Paragraph>
        ),
    },
];

export default HomePageOnboardingConfig;
