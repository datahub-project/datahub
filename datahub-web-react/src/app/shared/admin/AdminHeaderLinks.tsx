import styled from 'styled-components';
import * as React from 'react';
import {
    BankOutlined,
    BarChartOutlined,
    FolderOutlined,
    SettingOutlined,
    UsergroupAddOutlined,
} from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { Button } from 'antd';

import { useAppConfig } from '../../useAppConfig';
import { useGetAuthenticatedUser } from '../../useGetAuthenticatedUser';

const AdminLink = styled.span`
    margin-right: 4px;
`;

export function AdminHeaderLinks() {
    const me = useGetAuthenticatedUser();
    const { config } = useAppConfig();

    const isAnalyticsEnabled = config?.analyticsConfig.enabled;
    const isPoliciesEnabled = config?.policiesConfig.enabled;
    const isIdentityManagementEnabled = config?.identityManagementConfig.enabled;

    const showAnalytics = (isAnalyticsEnabled && me && me.platformPrivileges.viewAnalytics) || false;
    const showPolicyBuilder = (isPoliciesEnabled && me && me.platformPrivileges.managePolicies) || false;
    const showIdentityManagement =
        (isIdentityManagementEnabled && me && me.platformPrivileges.manageIdentities) || false;
    const showSettings = true;
    const showDomains = true; // Always show domains. TODO: Add a manage domains privilege check here.

    return (
        <>
            {showAnalytics && (
                <AdminLink>
                    <Link to="/analytics">
                        <Button type="text">
                            <BarChartOutlined /> Analytics
                        </Button>
                    </Link>
                </AdminLink>
            )}
            {showPolicyBuilder && (
                <AdminLink>
                    <Link to="/policies">
                        <Button type="text">
                            <BankOutlined /> Policies
                        </Button>
                    </Link>
                </AdminLink>
            )}
            {showDomains && (
                <AdminLink>
                    <Link to="/domains">
                        <Button type="text">
                            <FolderOutlined /> Domains
                        </Button>
                    </Link>
                </AdminLink>
            )}
            {showIdentityManagement && (
                <AdminLink>
                    <Link to="/identities">
                        <Button type="text">
                            <UsergroupAddOutlined /> Users & Groups
                        </Button>
                    </Link>
                </AdminLink>
            )}
            {showSettings && (
                <AdminLink>
                    <Link to="/settings">
                        <Button type="text">
                            <SettingOutlined /> Settings
                        </Button>
                    </Link>
                </AdminLink>
            )}
        </>
    );
}
