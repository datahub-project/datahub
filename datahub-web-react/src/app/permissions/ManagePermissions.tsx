import React from 'react';
import { PageTitle } from '@components';
import styled from 'styled-components';
import { RoutedTabs } from '../shared/RoutedTabs';
import { ManagePolicies } from './policy/ManagePolicies';
import { ManageRoles } from './roles/ManageRoles';

const PageContainer = styled.div`
    padding: 16px 20px;
    width: 100%;
    display: flex;
    flex-direction: column;
    overflow: auto;
`;

const Content = styled.div`
    &&& .ant-tabs-nav {
        margin: 0;
    }
    color: #262626;
    display: flex;
    flex-direction: column;
    overflow: auto;
`;

enum TabType {
    Roles = 'Roles',
    Policies = 'Policies',
}
const ENABLED_TAB_TYPES = [TabType.Roles, TabType.Policies];

export const ManagePermissions = () => {
    /**
     * Determines which view should be visible: roles or policies.
     */

    const getTabs = () => {
        return [
            {
                name: TabType.Roles,
                path: TabType.Roles.toLocaleLowerCase(),
                content: <ManageRoles />,
                display: {
                    enabled: () => true,
                },
            },
            {
                name: TabType.Policies,
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
            <PageTitle
                title="Manage Permissions"
                subTitle="View your DataHub permissions. Take administrative actions."
            />
            <Content>
                <RoutedTabs defaultPath={defaultTabPath} tabs={getTabs()} onTabChange={onTabChange} />
            </Content>
        </PageContainer>
    );
};
