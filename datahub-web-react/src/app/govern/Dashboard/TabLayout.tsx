import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { useHistory, useLocation } from 'react-router';
import { Tabs } from 'antd';
import { Button, Tooltip } from '@components';
import { PageTitle } from '@src/alchemy-components/components/PageTitle';
import analytics, { EventType } from '@src/app/analytics';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { useUserContext } from '../../context/useUserContext';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';
import { useAppConfig } from '../../useAppConfig';
import { useIsThemeV2 } from '../../useIsThemeV2';
import AnalyticsTab from './AnalyticsTab';
import { MissingPermissions } from './charts/AuxViews';
import { Header, Layout } from './components';
import FormsTab from './Forms/FormsTab';
import { PageRoutes } from '../../../conf/Global';
import { useGetFormsData } from './Forms/useGetFormsData';

const StyledTabs = styled(Tabs)<{ isThemeV2: boolean }>`
    flex: 1;
    overflow: hidden;

    .ant-tabs-tab {
        padding: 10px 20px;
        font-size: 14px;
        font-weight: 600;
        color: ${REDESIGN_COLORS.GREY_300};
    }

    ${(props) =>
        props.isThemeV2 &&
        `
        .ant-tabs-tab-active .ant-tabs-tab-btn {
            color: ${REDESIGN_COLORS.TITLE_PURPLE};
        }

        .ant-tabs-ink-bar {
            background-color: ${REDESIGN_COLORS.TITLE_PURPLE};
        }
    `}

    .ant-tabs-nav-wrap {
        margin: 0px 20px;
    }

    .ant-tabs-content-holder {
        display: flex;
    }

    .ant-tabs-tabpane {
        height: 100%;
    }
`;

const HeaderContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    width: 100%;
`;

const analyticsTab = {
    name: 'Analytics',
    key: 'analytics',
    component: <AnalyticsTab />,
};
const formsTab = {
    name: 'Forms',
    key: 'forms',
    component: <FormsTab />,
};

export const TabLayout = () => {
    const { platformPrivileges } = useUserContext();
    const { config } = useAppConfig();
    const { formCreationEnabled, showFormAnalytics } = config.featureFlags;
    const isThemeV2 = useIsThemeV2();
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const history = useHistory();
    const location = useLocation();
    const canEditForms = platformPrivileges?.manageDocumentationForms;

    const { TabPane } = Tabs;

    const searchParams = new URLSearchParams(location.search);

    const { inputs, searchData } = useGetFormsData();

    // Get the current documentationTab parameter
    const initialTab = searchParams.get('documentationTab') || '';

    const documentationTabs: any[] = [];
    if (formCreationEnabled) {
        documentationTabs.push(formsTab);
    }
    if (showFormAnalytics) {
        documentationTabs.push(analyticsTab);
    }

    const [currentTab, setCurrentTab] = useState(
        documentationTabs.some((tab) => tab.key === initialTab) ? initialTab : 'forms',
    );

    useEffect(() => {
        const params = new URLSearchParams(location.search);
        setCurrentTab(params.get('documentationTab') || currentTab);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [location.search]);

    useEffect(() => {
        const params = new URLSearchParams(location.search);
        params.set('documentationTab', currentTab);

        if (currentTab === 'forms') {
            params.delete('tab');
            params.delete('filter');
            params.delete('series');
        }

        // Update the URL without reloading the page
        history.replace({ search: params.toString() });
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [currentTab, history]);

    const handleTabChange = (tab) => {
        setCurrentTab(tab);
    };

    if (!documentationTabs.length) return null;

    if (!platformPrivileges?.manageDocumentationForms && !platformPrivileges?.viewDocumentationFormsPage)
        return <MissingPermissions />;

    // Render the dashboard
    return (
        <Layout $isShowNavBarRedesign={isShowNavBarRedesign}>
            <Header>
                <HeaderContainer>
                    <PageTitle
                        title="Compliance Forms"
                        subTitle="Create and manage compliance initiatives for your data assets"
                    />
                    {currentTab === 'forms' && (
                        <Tooltip
                            showArrow={false}
                            title={
                                !canEditForms
                                    ? 'Must have permission to manage forms. Ask your DataHub administrator.'
                                    : null
                            }
                        >
                            <>
                                <Button
                                    icon="Add"
                                    onClick={() => {
                                        analytics.event({
                                            type: EventType.CreateFormClickEvent,
                                        });
                                        history.push(PageRoutes.NEW_FORM, {
                                            inputs,
                                            searchAcrossEntities: searchData?.searchAcrossEntities,
                                        });
                                    }}
                                    disabled={!canEditForms}
                                >
                                    Create
                                </Button>
                            </>
                        </Tooltip>
                    )}
                </HeaderContainer>
            </Header>
            {documentationTabs.length > 1 ? (
                <StyledTabs activeKey={currentTab} isThemeV2={isThemeV2} onChange={handleTabChange}>
                    {documentationTabs.map((tab) => {
                        return (
                            <TabPane tab={tab.name} key={tab.key}>
                                {tab.component}
                            </TabPane>
                        );
                    })}
                </StyledTabs>
            ) : (
                <>{documentationTabs[0].component}</>
            )}
        </Layout>
    );
};
