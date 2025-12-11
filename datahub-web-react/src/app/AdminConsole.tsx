/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { BankOutlined, BarChartOutlined, MenuOutlined } from '@ant-design/icons';
import { Menu } from 'antd';
import Sider from 'antd/lib/layout/Sider';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { useAppConfig } from '@app/useAppConfig';

const ToggleContainer = styled.div`
    background-color: ${ANTD_GRAY[4]};
    border-top-right-radius: 2px;
    border-bottom-right-radius: 2px;
`;

const ControlMenu = styled(Menu)`
    padding-top: 28px;
    height: 100%;
`;

const ControlSlideOut = styled(Sider)`
    && {
        height: 100vh;
        position: fixed;
        left: 0px;
        z-index: 10000;
    }
`;

/**
 * Container for all views behind an authentication wall.
 */
export const AdminConsole = (): JSX.Element => {
    const me = useUserContext();

    const [adminConsoleOpen, setAdminConsoleOpen] = useState(false);
    const { config } = useAppConfig();

    const isAnalyticsEnabled = config?.analyticsConfig?.enabled;
    const isPoliciesEnabled = config?.policiesConfig?.enabled;

    const showAnalytics = (isAnalyticsEnabled && me && me?.platformPrivileges?.viewAnalytics) || false;
    const showPolicyBuilder = (isPoliciesEnabled && me && me?.platformPrivileges?.managePolicies) || false;
    const showAdminConsole = showAnalytics || showPolicyBuilder;

    const onMenuItemClick = () => {
        setAdminConsoleOpen(false);
    };

    const onCollapse = (collapsed) => {
        if (collapsed) {
            setAdminConsoleOpen(false);
        } else {
            setAdminConsoleOpen(true);
        }
    };

    const toggleView = (
        <ToggleContainer style={{}}>
            <MenuOutlined style={{ color: ANTD_GRAY[9] }} />
        </ToggleContainer>
    );

    return (
        <>
            {showAdminConsole && (
                <ControlSlideOut
                    zeroWidthTriggerStyle={{ top: '50%' }}
                    collapsible
                    collapsed={!adminConsoleOpen}
                    onCollapse={onCollapse}
                    collapsedWidth="0"
                    trigger={toggleView}
                >
                    <ControlMenu selectable={false} mode="inline" onSelect={onMenuItemClick}>
                        {showAnalytics && (
                            <Menu.Item key="analytics" icon={<BarChartOutlined />}>
                                <Link onClick={onMenuItemClick} to="/analytics">
                                    Analytics
                                </Link>
                            </Menu.Item>
                        )}
                        {showPolicyBuilder && (
                            <Menu.Item key="permissions" icon={<BankOutlined />}>
                                <Link onClick={onMenuItemClick} to="/permissions">
                                    Permissions
                                </Link>
                            </Menu.Item>
                        )}
                    </ControlMenu>
                </ControlSlideOut>
            )}
        </>
    );
};
