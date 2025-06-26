import { PageTitle, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import { HeaderContainer, PageContainer } from '@app/govern/structuredProperties/styledComponents';
import { AssertionsByAssertionSummary } from '@app/observe/dataset/assertion/AssertionsByAssertionSummary';
import { AssertionsByTableSummary } from '@app/observe/dataset/assertion/AssertionsByTableSummary';
import { IncidentsSummary } from '@app/observe/dataset/incident/IncidentsSummary';
import { useAppConfig } from '@app/useAppConfig';
import { Tabs } from '@src/alchemy-components/components/Tabs';
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

const Content = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    background-color: ${(props) => (props.$isShowNavBarRedesign ? 'white' : colors.white)};
    overflow: hidden;
    height: 100%;
`;

/**
 * The top-level Dataset Health Dashboard which lives under the Observe module.
 */
export const DatasetHealthPage = () => {
    const appConfig = useAppConfig();
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const [selectedTab, setSelectedTab] = React.useState<string>(ASSERTIONS_TAB_ID);
    const [selectedSubTab, setSelectedSubTab] = React.useState<string>(BY_ASSERTIONS_TAB_ID);

    // Initialize tab state based on current URL
    React.useEffect(() => {
        const currentUrl = window.location.pathname;
        if (currentUrl === BY_TABLE_URL) {
            setSelectedSubTab(BY_TABLE_TAB_ID);
        } else if (currentUrl === INCIDENTS_URL) {
            setSelectedTab(INCIDENTS_TAB_ID);
        }
    }, []);

    const handleMainTabChange = (tabKey: string) => {
        setSelectedTab(tabKey);
        if (tabKey === INCIDENTS_TAB_ID) {
            window.history.replaceState({}, '', INCIDENTS_URL);
        } else if (tabKey === ASSERTIONS_TAB_ID) {
            // When switching to assertions, preserve the current subtab
            const targetUrl = selectedSubTab === BY_TABLE_TAB_ID ? BY_TABLE_URL : BY_ASSERTIONS_URL;
            window.history.replaceState({}, '', targetUrl);
        }
    };

    const handleSubTabChange = (subtabKey: string) => {
        setSelectedSubTab(subtabKey);
        const targetUrl = subtabKey === BY_TABLE_TAB_ID ? BY_TABLE_URL : BY_ASSERTIONS_URL;
        window.history.replaceState({}, '', targetUrl);
    };

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
            onChange={handleSubTabChange}
            defaultTab={BY_ASSERTIONS_TAB_ID}
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
            onChange={handleMainTabChange}
            defaultTab={ASSERTIONS_TAB_ID}
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
