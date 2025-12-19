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

enum MainTab {
    Assertions = 'assertions',
    Incidents = 'incidents',
}

enum SubTab {
    ByAsset = 'by-asset',
    ByAssertions = 'by-assertions',
    ByIncidents = 'by-incidents',
}

const BASE_URL = '/observe/datasets';
const BASE_ASSERTIONS_URL = `${BASE_URL}/${MainTab.Assertions}`;
const BY_ASSERTIONS_URL = `${BASE_ASSERTIONS_URL}/${SubTab.ByAssertions}` as const;
const BY_ASSET_ASSERTIONS_URL = `${BASE_ASSERTIONS_URL}/${SubTab.ByAsset}` as const;
const BASE_INCIDENTS_URL = `${BASE_URL}/${MainTab.Incidents}`;
const BY_INCIDENTS_URL = `${BASE_INCIDENTS_URL}/${SubTab.ByIncidents}` as const;
const BY_ASSET_INCIDENTS_URL = `${BASE_INCIDENTS_URL}/${SubTab.ByAsset}` as const;

// Type union of all valid routes
type Route =
    | typeof BY_ASSET_ASSERTIONS_URL
    | typeof BY_ASSERTIONS_URL
    | typeof BY_ASSET_INCIDENTS_URL
    | typeof BY_INCIDENTS_URL;

// Array of all valid routes for runtime checking
const ALL_ROUTES = [BY_ASSET_ASSERTIONS_URL, BY_ASSERTIONS_URL, BY_ASSET_INCIDENTS_URL, BY_INCIDENTS_URL] as const;

function isRoute(value: string): value is Route {
    return ALL_ROUTES.includes(value as Route);
}

function isMainTab(value: string): value is MainTab {
    return Object.values(MainTab).includes(value as MainTab);
}

function isSubTab(value: string): value is SubTab {
    return Object.values(SubTab).includes(value as SubTab);
}

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

    const [selectedTab, setSelectedTab] = React.useState<MainTab>(MainTab.Assertions);
    const [selectedAssertionsSubTab, setSelectedAssertionsSubTab] = React.useState<SubTab>(SubTab.ByAsset);
    const [selectedIncidentsSubTab, setSelectedIncidentsSubTab] = React.useState<SubTab>(SubTab.ByAsset);
    const [isBulkCreateAssertionsDrawerOpen, setIsBulkCreateAssertionsDrawerOpen] = React.useState<boolean>(false);

    const assertionMonitorsEnabled = !!appConfig.config?.featureFlags?.assertionMonitorsEnabled;
    const onlineSmartAssertionsEnabled = !!appConfig.config?.featureFlags?.onlineSmartAssertionsEnabled;
    const showBulkCreateAssertionsButton =
        assertionMonitorsEnabled && onlineSmartAssertionsEnabled && selectedTab === MainTab.Assertions;

    // Initialize tab state based on current URL
    React.useEffect(() => {
        const currentUrl = location.pathname;
        if (!isRoute(currentUrl)) {
            return;
        }

        switch (currentUrl) {
            case BY_ASSET_ASSERTIONS_URL:
                setSelectedAssertionsSubTab(SubTab.ByAsset);
                setSelectedTab(MainTab.Assertions);
                break;
            case BY_ASSERTIONS_URL:
                setSelectedAssertionsSubTab(SubTab.ByAssertions);
                setSelectedTab(MainTab.Assertions);
                break;
            case BY_ASSET_INCIDENTS_URL:
                setSelectedIncidentsSubTab(SubTab.ByAsset);
                setSelectedTab(MainTab.Incidents);
                break;
            case BY_INCIDENTS_URL:
                setSelectedIncidentsSubTab(SubTab.ByIncidents);
                setSelectedTab(MainTab.Incidents);
                break;
            default:
                // All routes are handled above
                break;
        }
    }, [location.pathname]);

    const handleMainTabChange = (tabKey: string) => {
        if (!isMainTab(tabKey)) {
            return;
        }
        setSelectedTab(tabKey);
        switch (tabKey) {
            case MainTab.Incidents: {
                const targetUrl =
                    selectedIncidentsSubTab === SubTab.ByAsset ? BY_ASSET_INCIDENTS_URL : BY_INCIDENTS_URL;
                history.replace(targetUrl);
                break;
            }
            case MainTab.Assertions: {
                // When switching to assertions, preserve the current subtab
                const targetUrl =
                    selectedAssertionsSubTab === SubTab.ByAsset ? BY_ASSET_ASSERTIONS_URL : BY_ASSERTIONS_URL;
                history.replace(targetUrl);
                break;
            }
            default:
                // Should never reach here due to isMainTab type guard
                break;
        }
    };

    const handleAssertionsSubTabChange = (subtabKey: string) => {
        if (!isSubTab(subtabKey)) {
            return;
        }
        setSelectedAssertionsSubTab(subtabKey);
        switch (subtabKey) {
            case SubTab.ByAsset:
                history.replace(BY_ASSET_ASSERTIONS_URL);
                break;
            case SubTab.ByAssertions:
                history.replace(BY_ASSERTIONS_URL);
                break;
            case SubTab.ByIncidents:
                // Not applicable for assertions, but included for exhaustiveness
                break;
            default:
                // Should never reach here due to isSubTab type guard
                break;
        }
    };

    const handleIncidentsSubTabChange = (subtabKey: string) => {
        if (!isSubTab(subtabKey)) {
            return;
        }
        setSelectedIncidentsSubTab(subtabKey);
        switch (subtabKey) {
            case SubTab.ByAsset:
                history.replace(BY_ASSET_INCIDENTS_URL);
                break;
            case SubTab.ByIncidents:
                history.replace(BY_INCIDENTS_URL);
                break;
            case SubTab.ByAssertions:
                // Not applicable for incidents, but included for exhaustiveness
                break;
            default:
                // Should never reach here due to isSubTab type guard
                break;
        }
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
                    key: SubTab.ByAsset,
                    name: 'By Asset',
                },
                {
                    component: (
                        <Content $isShowNavBarRedesign={isShowNavBarRedesign}>
                            <AssertionsByAssertionSummary isAnomalyDetectionEnabled={showBulkCreateAssertionsButton} />
                        </Content>
                    ),
                    key: SubTab.ByAssertions,
                    name: 'By Assertion',
                },
            ]}
            secondary
            selectedTab={selectedAssertionsSubTab}
            onChange={handleAssertionsSubTabChange}
            defaultTab={SubTab.ByAsset}
        />
    );

    const incidentsTabs = (
        <Tabs
            styleOptions={{ containerHeight: 'full', navMarginTop: 12, navMarginBottom: 4 }}
            tabs={[
                {
                    component: <IncidentsSummary />,
                    key: SubTab.ByAsset,
                    name: 'By Asset',
                },
                {
                    component: <IncidentsByIncidentSummary />,
                    key: SubTab.ByIncidents,
                    name: 'By Incidents',
                },
            ]}
            secondary
            selectedTab={selectedIncidentsSubTab}
            onChange={handleIncidentsSubTabChange}
            defaultTab={SubTab.ByAsset}
        />
    );

    const mainTabs = (
        <Tabs
            styleOptions={{ navMarginBottom: 0 }}
            tabs={[
                {
                    component: <Content $isShowNavBarRedesign={isShowNavBarRedesign}>{assertionsTabs}</Content>,
                    key: MainTab.Assertions,
                    name: 'Assertions',
                },
                {
                    component: <Content $isShowNavBarRedesign={isShowNavBarRedesign}>{incidentsTabs}</Content>,
                    key: MainTab.Incidents,
                    name: 'Incidents',
                },
            ]}
            selectedTab={selectedTab}
            onChange={handleMainTabChange}
            defaultTab={MainTab.Assertions}
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
