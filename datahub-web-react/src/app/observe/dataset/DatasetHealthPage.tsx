import { PageTitle, Tabs, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { HeaderContainer, PageContainer } from '@app/govern/structuredProperties/styledComponents';
import { AssertionsSummary } from '@app/observe/dataset/assertion/AssertionsSummary';
import { IncidentsSummary } from '@app/observe/dataset/incident/IncidentsSummary';
import { useAppConfig } from '@app/useAppConfig';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import { useGetTotalDatasetsQuery } from '@graphql/dataset_health.generated';
import { EntityType } from '@types';

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
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        overflow-y: auto;
        height: calc(100% - 115px);
    `}
`;

const Section = styled.div`
    background-color: white;
    border-radius: 8px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    margin-bottom: 40px;
    border: 1px solid ${colors.gray[100]};
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
    // The total number of datasets in the instance (the denominator for metrics).
    const appConfig = useAppConfig();
    const userContext = useUserContext();
    const viewUrn = userContext.localState?.selectedViewUrn;
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const { main, sub } = getTabsFromUrl() || { main: ASSERTIONS_TAB_ID, sub: BY_ASSERTIONS_TAB_ID };
    const [selectedTab, setSelectedTab] = React.useState<string>(main);
    const [selectedSubTab, setSelectedSubTab] = React.useState<string>(sub || BY_ASSERTIONS_TAB_ID);

    const { data } = useGetTotalDatasetsQuery({
        variables: {
            input: {
                query: '*',
                types: [EntityType.Dataset],
                start: 0,
                count: 0,
                viewUrn,
            },
        },
    });

    const total = data?.searchAcrossEntities?.total || 0;

    const assertionsTabs = (
        <Tabs
            tabs={[
                {
                    component: <div>This is the content for Assertions - By Assertions</div>,
                    key: BY_ASSERTIONS_TAB_ID,
                    name: 'By Assertions',
                },
                {
                    component: <div>This is the content for Assertions - By Table</div>,
                    key: BY_TABLE_TAB_ID,
                    name: 'By Table',
                },
            ]}
            selectedTab={selectedSubTab}
            onChange={(selected) => {
                updateTabsAndUrl(selectedTab, selected, setSelectedTab, setSelectedSubTab);
            }}
        />
    );

    const mainTabs = (
        <Tabs
            tabs={[
                {
                    component: assertionsTabs,
                    key: ASSERTIONS_TAB_ID,
                    name: 'Assertions',
                },
                {
                    component: <div>This is the content for Incidents</div>,
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

    const datasetHealthDashboardV1 = (
        <>
            <Section>
                <IncidentsSummary total={total} />
            </Section>

            <Section>
                <AssertionsSummary total={total} />
            </Section>
        </>
    );

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            <HeaderContainer>
                <PageTitle title="Dataset Health" subTitle="Monitor the health of your organization's datasets" />
            </HeaderContainer>
            <Content $isShowNavBarRedesign={isShowNavBarRedesign}>
                {!!appConfig.config.featureFlags.datasetHealthDashboardV2Enabled && mainTabs}
                {!!appConfig.config.featureFlags.datasetHealthDashboardEnabled && datasetHealthDashboardV1}
            </Content>
        </PageContainer>
    );
};
