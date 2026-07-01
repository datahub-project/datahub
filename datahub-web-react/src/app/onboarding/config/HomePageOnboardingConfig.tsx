import { Heading, Text } from '@components';
import { Image } from 'antd';
import React from 'react';
import { Trans } from 'react-i18next';
import styled from 'styled-components';

import { OnboardingStep } from '@app/onboarding/OnboardingStep';

import dataHubFlowDiagram from '@images/datahub-flow-diagram-light.png';

const GLOBAL_WELCOME_TO_DATAHUB_ID = 'global-welcome-to-datahub';
export const HOME_PAGE_INGESTION_ID = 'home-page-ingestion';
export const HOME_PAGE_DOMAINS_ID = 'home-page-domains';
export const HOME_PAGE_DATA_PRODUCTS_ID = 'home-page-data-products';
export const HOME_PAGE_INSIGHTS_ID = 'home-page-insights';
export const HOME_PAGE_PLATFORMS_ID = 'home-page-platforms';
const HOME_PAGE_MOST_POPULAR_ID = 'home-page-most-popular';
const HOME_PAGE_SEARCH_BAR_ID = 'home-page-search-bar';
export const HOME_PAGE_ONBOARDING_CARDS_ID = 'home-page-onboarding-cards';

const InfoBox = styled.div`
    background-color: ${({ theme }) => theme.colors.bgSurfaceDarker};
    opacity: 0.7;
    border-radius: 4px;
    height: 40px;
    display: flex;
    align-items: center;
`;

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
                    src={dataHubFlowDiagram}
                />
                <Heading type="h3" size="2xl" weight="bold">
                    <Trans i18nKey="onboarding:home.welcomeTitle" />
                </Heading>
                <Text type="div" size="md">
                    <Trans i18nKey="onboarding:home.welcomeIntro" components={{ bold: <strong /> }} />
                </Text>
                <Text type="div" size="md">
                    <ul>
                        <li>
                            <Trans i18nKey="onboarding:home.bulletSearch" components={{ bold: <strong /> }} />
                        </li>
                        <li>
                            <Trans i18nKey="onboarding:home.bulletLineage" components={{ bold: <strong /> }} />
                        </li>
                        <li>
                            <Trans i18nKey="onboarding:home.bulletInsights" components={{ bold: <strong /> }} />
                        </li>
                        <li>
                            <Trans i18nKey="onboarding:home.bulletOwnership" components={{ bold: <strong /> }} />
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
        id: HOME_PAGE_INGESTION_ID,
        selector: `#${HOME_PAGE_INGESTION_ID}`,
        title: <Trans i18nKey="onboarding:home.ingestionTitle" />,
        content: (
            <Text type="div" size="md">
                <Trans i18nKey="onboarding:home.ingestionDescription" components={{ bold: <strong /> }} />
            </Text>
        ),
    },
    {
        id: HOME_PAGE_DOMAINS_ID,
        selector: `#${HOME_PAGE_DOMAINS_ID}`,
        title: <Trans i18nKey="onboarding:home.domainsTitle" />,
        content: (
            <Text type="div" size="md">
                <Trans i18nKey="onboarding:home.domainsDescription" components={{ bold: <strong /> }} />
            </Text>
        ),
    },
    {
        id: HOME_PAGE_PLATFORMS_ID,
        selector: `#${HOME_PAGE_PLATFORMS_ID}`,
        title: <Trans i18nKey="onboarding:home.platformsTitle" />,
        content: (
            <Text type="div" size="md">
                <Trans i18nKey="onboarding:home.platformsDescription" components={{ bold: <strong /> }} />
            </Text>
        ),
    },
    {
        id: HOME_PAGE_MOST_POPULAR_ID,
        selector: `#${HOME_PAGE_MOST_POPULAR_ID}`,
        title: <Trans i18nKey="onboarding:home.mostPopularTitle" />,
        content: <Trans i18nKey="onboarding:home.mostPopularDescription" />,
    },
    {
        id: HOME_PAGE_SEARCH_BAR_ID,
        selector: `#${HOME_PAGE_SEARCH_BAR_ID}`,
        title: <Trans i18nKey="onboarding:home.searchBarTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:home.searchBarDescription" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:home.exploreAll" components={{ bold: <strong /> }} />
                </p>
            </Text>
        ),
    },
];
