import { PageTitle, colors } from '@components';
import { Plus } from 'phosphor-react';
import React from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { HeaderContainer, PageContainer } from '@app/govern/structuredProperties/styledComponents';
import { AssertionsByAssertionSummary } from '@app/observe/dataset/assertion/AssertionsByAssertionSummary';
import { AssertionsByTableSummary } from '@app/observe/dataset/assertion/AssertionsByTableSummary';
import { IncidentsByIncidentSummary } from '@app/observe/dataset/incident/IncidentsByIncidentSummary';
import { IncidentsSummary } from '@app/observe/dataset/incident/IncidentsSummary';
import BulkCreateAssertionsDrawer from '@app/observe/shared/bulkCreate/BulkCreateAssertionsDrawer';
import { useAppConfig } from '@app/useAppConfig';
import { Tabs } from '@src/alchemy-components/components/Tabs';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

const BY_ASSERTIONS_TAB_ID = 'by-assertions';
const BY_ASSET_TAB_ID = 'by-asset';
const BY_INCIDENTS_TAB_ID = 'by-incidents';

const INCIDENTS_TAB_ID = 'incidents';
const ASSERTIONS_TAB_ID = 'assertions';

const BASE_URL = '/observe/datasets';
const BASE_ASSERTIONS_URL = `${BASE_URL}/${ASSERTIONS_TAB_ID}`;
const BY_ASSERTIONS_URL = `${BASE_ASSERTIONS_URL}/${BY_ASSERTIONS_TAB_ID}`;
const BY_ASSET_URL = `${BASE_ASSERTIONS_URL}/${BY_ASSET_TAB_ID}`;
const BASE_INCIDENTS_URL = `${BASE_URL}/${INCIDENTS_TAB_ID}`;
const BY_INCIDENTS_URL = `${BASE_INCIDENTS_URL}/${BY_INCIDENTS_TAB_ID}`;
const BY_ASSET_INCIDENTS_URL = `${BASE_INCIDENTS_URL}/${BY_ASSET_TAB_ID}`;

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

    const history = useHistory();
    const location = useLocation();

    const [selectedTab, setSelectedTab] = React.useState<string>(ASSERTIONS_TAB_ID);
    const [selectedAssertionsSubTab, setSelectedAssertionsSubTab] = React.useState<string>(BY_ASSET_TAB_ID);
    const [selectedIncidentsSubTab, setSelectedIncidentsSubTab] = React.useState<string>(BY_ASSET_TAB_ID);
    const [isBulkCreateAssertionsDrawerOpen, setIsBulkCreateAssertionsDrawerOpen] = React.useState<boolean>(false);

    const assertionMonitorsEnabled = !!appConfig.config?.featureFlags?.assertionMonitorsEnabled;
    const onlineSmartAssertionsEnabled = !!appConfig.config?.featureFlags?.onlineSmartAssertionsEnabled;
    const showBulkCreateAssertionsButton =
        assertionMonitorsEnabled && onlineSmartAssertionsEnabled && selectedTab === ASSERTIONS_TAB_ID;

    // Initialize tab state based on current URL
    React.useEffect(() => {
        const currentUrl = location.pathname;
        if (currentUrl === BY_ASSET_URL) {
            setSelectedAssertionsSubTab(BY_ASSET_TAB_ID);
        } else if (currentUrl === BY_ASSET_INCIDENTS_URL) {
            setSelectedIncidentsSubTab(BY_ASSET_TAB_ID);
            setSelectedTab(INCIDENTS_TAB_ID);
        } else if (currentUrl === BY_INCIDENTS_URL) {
            setSelectedIncidentsSubTab(BY_INCIDENTS_TAB_ID);
            setSelectedTab(INCIDENTS_TAB_ID);
        }
    }, [location.pathname]);

    const handleMainTabChange = (tabKey: string) => {
        setSelectedTab(tabKey);
        if (tabKey === INCIDENTS_TAB_ID) {
            const targetUrl = selectedIncidentsSubTab === BY_ASSET_TAB_ID ? BY_ASSET_INCIDENTS_URL : BY_INCIDENTS_URL;
            history.replace(targetUrl);
        } else if (tabKey === ASSERTIONS_TAB_ID) {
            // When switching to assertions, preserve the current subtab
            const targetUrl = selectedAssertionsSubTab === BY_ASSET_TAB_ID ? BY_ASSET_URL : BY_ASSERTIONS_URL;
            history.replace(targetUrl);
        }
    };

    const handleAssertionsSubTabChange = (subtabKey: string) => {
        setSelectedAssertionsSubTab(subtabKey);
        const targetUrl = subtabKey === BY_ASSET_TAB_ID ? BY_ASSET_URL : BY_ASSERTIONS_URL;
        history.replace(targetUrl);
    };

    const handleIncidentsSubTabChange = (subtabKey: string) => {
        setSelectedIncidentsSubTab(subtabKey);
        const targetUrl = subtabKey === BY_ASSET_TAB_ID ? BY_ASSET_INCIDENTS_URL : BY_INCIDENTS_URL;
        history.replace(targetUrl);
    };

    const assertionsTabs = (
        <Tabs
            styleOptions={{ containerHeight: 'full', navMarginTop: 12, navMarginBottom: 4 }}
            tabs={[
                {
                    component: (
                        <Content $isShowNavBarRedesign={isShowNavBarRedesign}>
                            <AssertionsByTableSummary isAnomalyDetectionEnabled={showBulkCreateAssertionsButton} />
                        </Content>
                    ),
                    key: BY_ASSET_TAB_ID,
                    name: 'By Asset',
                },
                {
                    component: (
                        <Content $isShowNavBarRedesign={isShowNavBarRedesign}>
                            <AssertionsByAssertionSummary isAnomalyDetectionEnabled={showBulkCreateAssertionsButton} />
                        </Content>
                    ),
                    key: BY_ASSERTIONS_TAB_ID,
                    name: 'By Assertion',
                },
            ]}
            secondary
            selectedTab={selectedAssertionsSubTab}
            onChange={handleAssertionsSubTabChange}
            defaultTab={BY_ASSET_TAB_ID}
        />
    );

    const incidentsTabs = (
        <Tabs
            styleOptions={{ containerHeight: 'full', navMarginTop: 12, navMarginBottom: 4 }}
            tabs={[
                {
                    component: <IncidentsSummary />,
                    key: BY_ASSET_TAB_ID,
                    name: 'By Asset',
                },
                {
                    component: <IncidentsByIncidentSummary />,
                    key: BY_INCIDENTS_TAB_ID,
                    name: 'By Incidents',
                },
            ]}
            secondary
            selectedTab={selectedIncidentsSubTab}
            onChange={handleIncidentsSubTabChange}
            defaultTab={BY_ASSET_TAB_ID}
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
                    component: <Content $isShowNavBarRedesign={isShowNavBarRedesign}>{incidentsTabs}</Content>,
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
                    actionButton={
                        showBulkCreateAssertionsButton
                            ? {
                                  label: 'Bulk Create',
                                  onClick: () => {
                                      setIsBulkCreateAssertionsDrawerOpen(true);
                                      analytics.event({
                                          type: EventType.ClickBulkCreateAssertion,
                                          surface: 'dataset-health',
                                      });
                                  },
                                  icon: <Plus size={14} />,
                              }
                            : undefined
                    }
                />
            </HeaderContainer>
            {!!appConfig.config.featureFlags.datasetHealthDashboardEnabled && mainTabs}
            <BulkCreateAssertionsDrawer
                key={`${isBulkCreateAssertionsDrawerOpen}`}
                open={isBulkCreateAssertionsDrawerOpen}
                onClose={() => setIsBulkCreateAssertionsDrawerOpen(false)}
            />
        </PageContainer>
    );
};
