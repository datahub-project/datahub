import { Tabs, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import { useHistory, useLocation } from 'react-router';
import styled from 'styled-components';
import { useUserContext } from '../../context/useUserContext';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';
import { useAppConfig } from '../../useAppConfig';
import { useIsThemeV2 } from '../../useIsThemeV2';
import AnalyticsTab from './AnalyticsTab';
import FormsTab from './Forms/FormsTab';
import { MissingPermissions } from './charts/AuxViews';
import { Header, Layout } from './components';

const StyledTabs = styled(Tabs)<{ isThemeV2: boolean }>`
    height: 100%;
    flex: 1;

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

    .ant-tabs-nav {
        margin: 0;
    }

    .ant-tabs-content-holder {
        display: flex;
    }

    .ant-tabs-tabpane {
        height: 100%;
    }
`;

export const PageHeading = styled(Typography.Text)`
    font-size: 24px;
    font-weight: 700;
    color: ${REDESIGN_COLORS.TEXT_HEADING_SUB_LINK};
`;

const documentationTabs = [
    {
        name: 'Analytics',
        key: 'analytics',
        component: <AnalyticsTab />,
    },
    {
        name: 'Forms',
        key: 'forms',
        component: <FormsTab />,
    },
];

export const TabLayout = () => {
    const { platformPrivileges } = useUserContext();
    const { config } = useAppConfig();
    const { formCreationEnabled } = config.featureFlags;
    const isThemeV2 = useIsThemeV2();
    const history = useHistory();
    const location = useLocation();

    const { TabPane } = Tabs;

    const searchParams = new URLSearchParams(location.search);

    // Get the current documentationTab parameter
    const initialTab = searchParams.get('documentationTab') || '';

    const [currentTab, setCurrentTab] = useState(
        documentationTabs.some((tab) => tab.key === initialTab) ? initialTab : 'analytics',
    );

    useEffect(() => {
        const params = new URLSearchParams(location.search);
        params.set('documentationTab', currentTab);

        // Update the URL without reloading the page
        history.replace({ search: params.toString() });
    }, [currentTab, history, location.search]);

    const handleTabChange = (tab) => {
        setCurrentTab(tab);
    };

    if (!platformPrivileges?.manageDocumentationForms) return <MissingPermissions />;

    // Render the dashboard
    return (
        <Layout>
            <Header>
                <PageHeading>Documentation</PageHeading>
            </Header>
            {formCreationEnabled ? (
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
                <AnalyticsTab />
            )}
        </Layout>
    );
};
