import styled from 'styled-components';
import * as React from 'react';
import { BankOutlined, BarChartOutlined } from '@ant-design/icons';
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

    const showAnalytics = (isAnalyticsEnabled && me && me.platformPrivileges.viewAnalytics) || false;
    const showPolicyBuilder = (isPoliciesEnabled && me && me.platformPrivileges.managePolicies) || false;

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
        </>
    );
}
