/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ManagePolicies } from '@app/permissions/policy/ManagePolicies';
import { ManageRoles } from '@app/permissions/roles/ManageRoles';
import { RoutedTabs } from '@app/shared/RoutedTabs';

const PageContainer = styled.div`
    padding-top: 20px;
    width: 100%;
    display: flex;
    flex-direction: column;
    overflow: auto;
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
    display: flex;
    flex-direction: column;
    overflow: auto;

    &&& .ant-tabs > .ant-tabs-nav .ant-tabs-nav-wrap {
        padding-left: 28px;
    }
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
            <PageHeaderContainer>
                <PageTitle level={3}>Manage Permissions</PageTitle>
                <Typography.Paragraph type="secondary">
                    View your DataHub permissions. Take administrative actions.
                </Typography.Paragraph>
            </PageHeaderContainer>
            <Content>
                <RoutedTabs defaultPath={defaultTabPath} tabs={getTabs()} onTabChange={onTabChange} />
            </Content>
        </PageContainer>
    );
};
