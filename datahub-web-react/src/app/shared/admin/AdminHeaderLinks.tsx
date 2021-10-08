import styled from 'styled-components';
import * as React from 'react';
import { BankOutlined, BarChartOutlined, InboxOutlined, UsergroupAddOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { Button } from 'antd';
import { useAppConfig } from '../../useAppConfig';
import { useGetAuthenticatedUser } from '../../useGetAuthenticatedUser';

const AdminLink = styled.span`
    margin-right: 4px;
`;

const LinkButton = styled(Button)`
    color: white;
`;

export function AdminHeaderLinks() {
    const me = useGetAuthenticatedUser();
    const { config } = useAppConfig();

    const isAnalyticsEnabled = config?.analyticsConfig.enabled;
    const isPoliciesEnabled = config?.policiesConfig.enabled;
    // Currently we only have a flag for metadata proposals.
    // In the future, we may add configs for alerts, announcements, etc.
    const isActionRequestsEnabled = config?.actionRequestsConfig.enabled;
    const isIdentityManagementEnabled = config?.identityManagementConfig.enabled;

    const showAnalytics = (isAnalyticsEnabled && me && me.platformPrivileges.viewAnalytics) || false;
    const showPolicyBuilder = (isPoliciesEnabled && me && me.platformPrivileges.managePolicies) || false;
    const showActionRequests = (isActionRequestsEnabled && me && me.platformPrivileges.viewMetadataProposals) || false;
    const showIdentityManagement =
        (isIdentityManagementEnabled && me && me.platformPrivileges.manageIdentities) || false;

    return (
        <>
            {showAnalytics && (
                <AdminLink>
                    <Link to="/analytics">
                        <LinkButton type="text">
                            <BarChartOutlined /> Analytics
                        </LinkButton>
                    </Link>
                </AdminLink>
            )}
            {showPolicyBuilder && (
                <AdminLink>
                    <Link to="/policies">
                        <LinkButton type="text">
                            <BankOutlined /> Policies
                        </LinkButton>
                    </Link>
                </AdminLink>
            )}
            {showActionRequests && (
                <AdminLink>
                    <Link to="/requests">
                        <LinkButton type="text">
                            <InboxOutlined /> My Requests
                        </LinkButton>
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
        </>
    );
}
