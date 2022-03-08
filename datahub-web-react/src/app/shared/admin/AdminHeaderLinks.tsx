import styled from 'styled-components';
import * as React from 'react';
import {
    ApiOutlined,
    BankOutlined,
    BarChartOutlined,
    InboxOutlined,
    SettingOutlined,
    UsergroupAddOutlined,
    FolderOutlined,
} from '@ant-design/icons';
import { Link } from 'react-router-dom';
import { Button } from 'antd';
import { useAppConfig } from '../../useAppConfig';
import { useGetAuthenticatedUser } from '../../useGetAuthenticatedUser';

const AdminLink = styled.span`
    margin-right: 4px;
    &&& .ant-btn-text {
        color: ${(props) => props.theme.styles['heading-color']};
        :hover {
            color: ${(props) => props.theme.styles['primary-color']};
        }
    }
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
    const isIngestionEnabled = config?.managedIngestionConfig.enabled;

    const showAnalytics = (isAnalyticsEnabled && me && me.platformPrivileges.viewAnalytics) || false;
    const showPolicyBuilder = (isPoliciesEnabled && me && me.platformPrivileges.managePolicies) || false;
    const showActionRequests = (isActionRequestsEnabled && me && me.platformPrivileges.viewMetadataProposals) || false;
    const showIdentityManagement =
        (isIdentityManagementEnabled && me && me.platformPrivileges.manageIdentities) || false;
    const showSettings = true;
    const showIngestion =
        isIngestionEnabled && me && me.platformPrivileges.manageIngestion && me.platformPrivileges.manageSecrets;
    const showDomains = me?.platformPrivileges?.manageDomains || false;

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
            {showActionRequests && (
                <AdminLink>
                    <Link to="/requests">
                        <Button type="text">
                            <InboxOutlined /> My Requests
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
            {showIngestion && (
                <AdminLink>
                    <Link to="/ingestion">
                        <Button type="text">
                            <ApiOutlined /> Ingestion
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
            {showSettings && (
                <AdminLink style={{ marginRight: 16 }}>
                    <Link to="/settings">
                        <Button type="text">
                            <SettingOutlined />
                        </Button>
                    </Link>
                </AdminLink>
            )}
        </>
    );
}
