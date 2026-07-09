import { Heading, Text } from '@components';
import { Image } from 'antd';
import React from 'react';
import { Trans } from 'react-i18next';
import styled from 'styled-components';

import { OnboardingStep } from '@app/onboarding/OnboardingStep';

import dataHubFlowDiagram from '@images/datahub-flow-diagram-light.png';

export const V2_SEARCH_BAR_ID = 'v2-search-bar';
export const V2_HOME_PAGE_MOST_POPULAR_ID = 'v2-home-page-most-popular';
export const V2_SEARCH_BAR_VIEWS = 'v2-search-bar-views';
export const V2_HOME_PAGE_DISCOVER_ID = 'v2-home-page-discover';
export const V2_HOME_PAGE_ANNOUNCEMENTS_ID = 'v2-home-page-announcements';
export const V2_HOME_PAGE_PERSONAL_SIDEBAR_ID = 'v2-home-page-personal-sidebar';
export const V2_HOME_PAGE_PENDING_TASKS_ID = 'v2-home-page-pending-tasks';
export const GLOBAL_WELCOME_TO_ACRYL_ID = 'global-welcome-to-acryl';

const InfoBox = styled.div`
    background-color: ${({ theme }) => theme.colors.bgSurfaceDarker};
    opacity: 0.7;
    border-radius: 4px;
    height: 40px;
    display: flex;
    align-items: center;
`;

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
                <Heading type="h3" size="2xl" weight="bold">
                    <Trans i18nKey="onboarding:home.welcomeTitleV2" />
                </Heading>
                <Text type="div" size="md">
                    <Trans i18nKey="onboarding:home.welcomeIntroV2" components={{ bold: <strong /> }} />
                </Text>
                <Text type="div" size="md">
                    <ul>
                        <li>
                            <Trans i18nKey="onboarding:home.bulletSearchV2" components={{ bold: <strong /> }} />
                        </li>
                        <li>
                            <Trans i18nKey="onboarding:home.bulletQuality" components={{ bold: <strong /> }} />
                        </li>
                        <li>
                            <Trans i18nKey="onboarding:home.bulletLineageV2" components={{ bold: <strong /> }} />
                        </li>
                        <li>
                            <Trans i18nKey="onboarding:home.bulletInsights" components={{ bold: <strong /> }} />
                        </li>
                        <li>
                            <Trans i18nKey="onboarding:home.bulletOwnership" components={{ bold: <strong /> }} />
                        </li>
                        <li>
                            <Trans i18nKey="onboarding:home.bulletGovernance" components={{ bold: <strong /> }} />
                        </li>
                    </ul>
                    <p>
                        <Trans i18nKey="onboarding:home.letsGetStarted" />
                    </p>
                    <InfoBox>
                        {/* eslint-disable-next-line i18next/no-literal-string -- (untranslated-text) decorative emoji */}
                        <span style={{ paddingLeft: '5px' }}>💡</span>
                        <span style={{ paddingLeft: '10px' }}>
                            <Trans i18nKey="onboarding:home.tutorialHotkey" components={{ bold: <strong /> }} />
                        </span>
                    </InfoBox>
                </Text>
            </div>
        ),
        style: { minWidth: '650px' },
    },
    {
        id: V2_SEARCH_BAR_ID,
        selector: `#${V2_SEARCH_BAR_ID}`,
        title: <Trans i18nKey="onboarding:home.searchBarTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:home.searchBarDescription" components={{ bold: <strong /> }} />
                </p>
            </Text>
        ),
    },
    {
        id: V2_SEARCH_BAR_VIEWS,
        selector: `#${V2_SEARCH_BAR_VIEWS}`,
        title: <Trans i18nKey="onboarding:home.viewsTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:home.viewsDescription" />
                </p>
            </Text>
        ),
    },
    {
        id: V2_HOME_PAGE_MOST_POPULAR_ID,
        selector: `#${V2_HOME_PAGE_MOST_POPULAR_ID}`,
        title: <Trans i18nKey="onboarding:home.mostPopularTitle" />,
        content: <Trans i18nKey="onboarding:home.mostPopularDescription" />,
    },
    {
        id: V2_HOME_PAGE_DISCOVER_ID,
        selector: `#${V2_HOME_PAGE_DISCOVER_ID}`,
        title: <Trans i18nKey="onboarding:home.discoverTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:home.discoverDescription" components={{ bold: <strong /> }} />
                </p>
            </Text>
        ),
    },
    {
        id: V2_HOME_PAGE_ANNOUNCEMENTS_ID,
        selector: `#${V2_HOME_PAGE_ANNOUNCEMENTS_ID}`,
        title: <Trans i18nKey="onboarding:home.announcementsTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:home.announcementsDescription" components={{ bold: <strong /> }} />
                </p>
            </Text>
        ),
    },
    {
        id: V2_HOME_PAGE_PERSONAL_SIDEBAR_ID,
        selector: `#${V2_HOME_PAGE_PERSONAL_SIDEBAR_ID}`,
        title: <Trans i18nKey="onboarding:home.personalSidebarTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:home.personalSidebarDescription" components={{ bold: <strong /> }} />
                </p>
            </Text>
        ),
    },
];

export default HomePageOnboardingConfig;
