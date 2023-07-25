import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import { RoutedTabs } from '../shared/RoutedTabs';
import { ManagePolicies } from './policy/ManagePolicies';
import { ManageRoles } from './roles/ManageRoles';

const PageContainer = styled.div`
    padding-top: 20px;
    width: 100%;
`;

const PageHeaderContainer = styled.div`
    && {
        padding-left: 24px;
    }
`;

const PageTitle = styled(Typography.Title)`
    && {
        margin-bottom: 12px;
    }
`;

const Content = styled.div`
    &&& .ant-tabs-nav {
        margin: 0;
    }
    color: #262626;
    height: calc(100vh - 60px);

    &&& .ant-tabs > .ant-tabs-nav .ant-tabs-nav-wrap {
        padding-left: 28px;
    }
`;

enum TabType {
    Roles = 'Roles',
    Policies = 'Policies',
    RolesZh = '角色',
    PoliciesZh = '规则',
}
const ENABLED_TAB_TYPES = [TabType.RolesZh, TabType.PoliciesZh];

export const ManagePermissions = () => {
    /**
     * Determines which view should be visible: roles or policies.
     */

    const getTabs = () => {
        return [
            {
                name: TabType.RolesZh,
                path: TabType.Roles.toLocaleLowerCase(),
                content: <ManageRoles />,
                display: {
                    enabled: () => true,
                },
            },
            {
                name: TabType.PoliciesZh,
                path: TabType.Policies.toLocaleLowerCase(),
                content: <ManagePolicies />,
                display: {
                    enabled: () => true,
                },
            },
        ].filter((tab) => ENABLED_TAB_TYPES.includes(tab.name));
    };

    const defaultTabPath = getTabs() && getTabs()?.length > 0 ? getTabs()[0].path : '';
    const onTabChange = () => null;

    return (
        <PageContainer>
            <PageHeaderContainer>
                <PageTitle level={3}>权限管理</PageTitle>
                <Typography.Paragraph type="secondary">
                    查看与管理您的DataHub权限.
                </Typography.Paragraph>
            </PageHeaderContainer>
            <Content>
                <RoutedTabs defaultPath={defaultTabPath} tabs={getTabs()} onTabChange={onTabChange} />
            </Content>
        </PageContainer>
    );
};
