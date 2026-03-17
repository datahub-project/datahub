import { PageTitle } from '@components';
import React from 'react';
import styled from 'styled-components';

import { ManagePolicies } from '@app/permissions/policy/ManagePolicies';
import { ManageRoles } from '@app/permissions/roles/ManageRoles';
import { AlchemyRoutedTabs } from '@app/shared/AlchemyRoutedTabs';

const PageContainer = styled.div`
    padding: 16px 20px;
    width: 100%;
    flex: 1;
    display: flex;
    gap: 16px;
    flex-direction: column;
    overflow: hidden;
`;

const PageHeaderContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

const Content = styled.div`
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
    overflow: hidden;

    &&& .ant-tabs-nav {
        margin-bottom: 0;
    }
`;

enum TabType {
    Roles = 'Roles',
    Policies = 'Policies',
}
const ENABLED_TAB_TYPES = [TabType.Roles, TabType.Policies];

export const ManagePermissions = () => {
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

    return (
        <PageContainer>
            <PageHeaderContainer>
                <PageTitle
                    title="Manage Permissions"
                    subTitle="View your DataHub permissions. Take administrative actions."
                />
            </PageHeaderContainer>
            <Content>
                <AlchemyRoutedTabs defaultPath={defaultTabPath} tabs={getTabs()} />
            </Content>
        </PageContainer>
    );
};
