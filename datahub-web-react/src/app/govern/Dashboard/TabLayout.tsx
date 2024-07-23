import React from 'react';

import { Tabs, Typography } from 'antd';
import styled from 'styled-components';
import { useUserContext } from '../../context/useUserContext';
import { Layout, Header } from './components';
import FormsTab from './Forms/FormsTab';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';
import { useAppConfig } from '../../useAppConfig';
import AnalyticsTab from './AnalyticsTab';
import { MissingPermissions } from './charts/AuxViews';
import { useIsThemeV2 } from '../../useIsThemeV2';

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

export const TabLayout = () => {
    const { platformPrivileges } = useUserContext();
    const { config } = useAppConfig();
    const { formCreationEnabled } = config.featureFlags;
    const isThemeV2 = useIsThemeV2();

    if (!platformPrivileges?.manageDocumentationForms) return <MissingPermissions />;

    const { TabPane } = Tabs;

    // Render the dashboard
    return (
        <Layout>
            <Header>
                <PageHeading>Documentation</PageHeading>
            </Header>
            {formCreationEnabled ? (
                <StyledTabs defaultActiveKey="1" isThemeV2={isThemeV2}>
                    <TabPane tab="Analytics" key="1">
                        <AnalyticsTab />
                    </TabPane>
                    <TabPane tab="Forms" key="2">
                        <FormsTab />
                    </TabPane>
                </StyledTabs>
            ) : (
                <AnalyticsTab />
            )}
        </Layout>
    );
};
