import { PageTitle, Tabs, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import { HeaderContainer, PageContainer } from '@app/govern/structuredProperties/styledComponents';
import { AssertionsByAssertionSummary } from '@app/observe/dataset/assertion/AssertionsByAssertionSummary';
import { AssertionsByTableSummary } from '@app/observe/dataset/assertion/AssertionsByTableSummary';
import { IncidentsSummary } from '@app/observe/dataset/incident/IncidentsSummary';
import { useAppConfig } from '@app/useAppConfig';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

const BY_ASSERTIONS_TAB_ID = 'by-assertions';
const BY_TABLE_TAB_ID = 'by-table';
const INCIDENTS_TAB_ID = 'incidents';
const ASSERTIONS_TAB_ID = 'assertions';

const BASE_URL = '/observe/datasets';
const BASE_ASSERTIONS_URL = `${BASE_URL}/${ASSERTIONS_TAB_ID}`;
const BY_ASSERTIONS_URL = `${BASE_ASSERTIONS_URL}/${BY_ASSERTIONS_TAB_ID}`;
const BY_TABLE_URL = `${BASE_ASSERTIONS_URL}/${BY_TABLE_TAB_ID}`;
const INCIDENTS_URL = `${BASE_URL}/${INCIDENTS_TAB_ID}`;

const TABS_TO_URL_MAP: Record<string, string> = {
    [ASSERTIONS_TAB_ID]: BY_ASSERTIONS_URL, // default to 'by-assertions'
    [INCIDENTS_TAB_ID]: INCIDENTS_URL,
    [BY_ASSERTIONS_TAB_ID]: BY_ASSERTIONS_URL,
    [BY_TABLE_TAB_ID]: BY_TABLE_URL,
};

type TabKeys = {
    main: string;
    sub: string | null;
};

const URL_TO_TAB_MAP: Record<string, TabKeys> = {
    [BASE_URL]: { main: ASSERTIONS_TAB_ID, sub: BY_ASSERTIONS_TAB_ID }, // default to 'by-assertions'
    [BASE_ASSERTIONS_URL]: { main: ASSERTIONS_TAB_ID, sub: BY_ASSERTIONS_TAB_ID }, // default to 'by-assertions'
    [BY_ASSERTIONS_URL]: { main: ASSERTIONS_TAB_ID, sub: BY_ASSERTIONS_TAB_ID },
    [BY_TABLE_URL]: { main: ASSERTIONS_TAB_ID, sub: BY_TABLE_TAB_ID },
    [INCIDENTS_URL]: { main: INCIDENTS_TAB_ID, sub: null },
};

const Content = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    background-color: ${(props) => (props.$isShowNavBarRedesign ? 'white' : colors.white)};
    overflow: hidden;
    height: 100%;
`;

/**
 * Extracts the main and sub-tab from the current URL.
 * @returns An object containing the main tab and sub-tab.
 */
const getTabsFromUrl = (): TabKeys | undefined => {
    return URL_TO_TAB_MAP[window.location.pathname];
};

/**
 * Extracts the URL from the selected tab and sub-tab.
 * @param main - The selected main tab.
 * @param sub - The selected sub-tab (if any).
 * @returns The URL corresponding to the selected tab and sub-tab.
 */
const getUrlFromTabs = ({ main, sub }: TabKeys): string => {
    if (main === ASSERTIONS_TAB_ID) {
        return sub ? TABS_TO_URL_MAP[sub] : TABS_TO_URL_MAP[main];
    }
    return TABS_TO_URL_MAP[main];
};

/**
 * Updates the URL based on the selected tab and sub-tab.
 * @param main - The selected main tab.
 * @param sub - The selected sub-tab (if any).
 */
const updateUrl = ({ main, sub }: TabKeys) => {
    const url = getUrlFromTabs({ main, sub });
    window.history.replaceState({}, '', url);
};

/**
 * Updates the selected tab and sub-tab in the state and URL.
 * @param selectedTab - The selected main tab.
 * @param selectedSubTab - The selected sub-tab.
 * @param setSelectedTab - The state setter for the main tab.
 * @param setSelectedSubTab - The state setter for the sub-tab.
 */
const updateTabsAndUrl = (
    selectedTab: string,
    selectedSubTab: string,
    setSelectedTab: React.Dispatch<React.SetStateAction<string>>,
    setSelectedSubTab: React.Dispatch<React.SetStateAction<string>>,
) => {
    setSelectedTab(selectedTab);
    setSelectedSubTab(selectedSubTab);
    updateUrl({ main: selectedTab, sub: selectedSubTab });
};

/**
 * The top-level Dataset Health Dashboard which lives under the Observe module.
 */
export const DatasetHealthPage = () => {
    const appConfig = useAppConfig();
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const { main, sub } = getTabsFromUrl() || { main: ASSERTIONS_TAB_ID, sub: BY_ASSERTIONS_TAB_ID };
    const [selectedTab, setSelectedTab] = React.useState<string>(main);
    const [selectedSubTab, setSelectedSubTab] = React.useState<string>(sub || BY_ASSERTIONS_TAB_ID);

    const assertionsTabs = (
        <Tabs
            styleOptions={{ containerHeight: 'full', navMarginTop: 12, navMarginBottom: 4 }}
            tabs={[
                {
                    component: (
                        <Content $isShowNavBarRedesign={isShowNavBarRedesign}>
                            <AssertionsByAssertionSummary />
                        </Content>
                    ),
                    key: BY_ASSERTIONS_TAB_ID,
                    name: 'By Assertion',
                },
                {
                    component: (
                        <Content $isShowNavBarRedesign={isShowNavBarRedesign}>
                            <AssertionsByTableSummary />
                        </Content>
                    ),
                    key: BY_TABLE_TAB_ID,
                    name: 'By Asset',
                },
            ]}
            secondary
            selectedTab={selectedSubTab}
            onChange={(selected) => {
                updateTabsAndUrl(selectedTab, selected, setSelectedTab, setSelectedSubTab);
            }}
        />
    );

    const mainTabs = (
        <Tabs
            styleOptions={{ navMarginBottom: 0 }}
            tabs={[
                {
                    component: <Content $isShowNavBarRedesign={isShowNavBarRedesign}>{assertionsTabs}</Content>,
                    key: ASSERTIONS_TAB_ID,
                    name: 'Assertions',
                },
                {
                    component: (
                        <Content $isShowNavBarRedesign={isShowNavBarRedesign}>
                            <IncidentsSummary />
                        </Content>
                    ),
                    key: INCIDENTS_TAB_ID,
                    name: 'Incidents',
                },
            ]}
            selectedTab={selectedTab}
            onChange={(selected) => {
                updateTabsAndUrl(selected, selectedSubTab, setSelectedTab, setSelectedSubTab);
            }}
        />
    );

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign} style={{ paddingBottom: 0 }}>
            <HeaderContainer>
                <PageTitle
                    title="Data Health"
                    subTitle="A birds-eye view of the health of your entire data landscape."
                />
            </HeaderContainer>
            {!!appConfig.config.featureFlags.datasetHealthDashboardEnabled && mainTabs}
        </PageContainer>
    );
};
