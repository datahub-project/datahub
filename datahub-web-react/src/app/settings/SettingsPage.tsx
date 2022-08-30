import React from 'react';
import { Menu, Typography, Divider } from 'antd';
import { BankOutlined, SafetyCertificateOutlined, UsergroupAddOutlined, ToolOutlined } from '@ant-design/icons';
import { Redirect, Route, useHistory, useLocation, useRouteMatch, Switch } from 'react-router';
import styled from 'styled-components';
import { ANTD_GRAY } from '../entity/shared/constants';
import { ManageIdentities } from '../identity/ManageIdentities';
import { ManagePolicies } from '../policy/ManagePolicies';
import { useAppConfig } from '../useAppConfig';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import { AccessTokens } from './AccessTokens';
import { Preferences } from './Preferences';

const PageContainer = styled.div`
    display: flex;
`;

const SettingsBarContainer = styled.div`
    padding-top: 20px;
    min-height: 100vh;
    border-right: 1px solid ${ANTD_GRAY[5]};
`;

const SettingsBarHeader = styled.div`
    && {
        padding-left: 24px;
    }
    margin-bottom: 20px;
`;

const PageTitle = styled(Typography.Title)`
    && {
        margin-bottom: 8px;
    }
`;

const ThinDivider = styled(Divider)`
    padding: 0px;
    margin: 0px;
`;

const ItemTitle = styled.span`
    margin-left: 8px;
`;

/**
 * URL Paths for each settings page.
 */
const PATHS = [
    { path: 'tokens', content: <AccessTokens /> },
    { path: 'identities', content: <ManageIdentities /> },
    { path: 'policies', content: <ManagePolicies /> },
    { path: 'preferences', content: <Preferences /> },
];

/**
 * The default selected path
 */
const DEFAULT_PATH = PATHS[0];

export const SettingsPage = () => {
    const { path, url } = useRouteMatch();
    const { pathname } = useLocation();
    const history = useHistory();
    const subRoutes = PATHS.map((p) => p.path.replace('/', ''));
    const currPathName = pathname.replace(path, '');
    const trimmedPathName = currPathName.endsWith('/') ? pathname.slice(0, pathname.length - 1) : currPathName;
    const splitPathName = trimmedPathName.split('/');
    const providedPath = splitPathName[1];
    const activePath = subRoutes.includes(providedPath) ? providedPath : DEFAULT_PATH.path.replace('/', '');

    const me = useGetAuthenticatedUser();
    const { config } = useAppConfig();

    const isPoliciesEnabled = config?.policiesConfig.enabled;
    const isIdentityManagementEnabled = config?.identityManagementConfig.enabled;

    const showPolicies = (isPoliciesEnabled && me && me.platformPrivileges.managePolicies) || false;
    const showUsersGroups = (isIdentityManagementEnabled && me && me.platformPrivileges.manageIdentities) || false;

    return (
        <PageContainer>
            <SettingsBarContainer>
                <SettingsBarHeader>
                    <PageTitle level={3}>Settings</PageTitle>
                    <Typography.Paragraph type="secondary">Manage your DataHub settings.</Typography.Paragraph>
                </SettingsBarHeader>
                <ThinDivider />
                <Menu
                    selectable={false}
                    mode="inline"
                    style={{ width: 256, marginTop: 8 }}
                    selectedKeys={[activePath]}
                    onClick={(newPath) => {
                        history.push(`${url}/${newPath.key}`);
                    }}
                >
                    <Menu.ItemGroup title="Developer">
                        <Menu.Item key="tokens">
                            <SafetyCertificateOutlined />
                            <ItemTitle>Access Tokens</ItemTitle>
                        </Menu.Item>
                    </Menu.ItemGroup>
                    {(showPolicies || showUsersGroups) && (
                        <Menu.ItemGroup title="Access">
                            {showUsersGroups && (
                                <Menu.Item key="identities">
                                    <UsergroupAddOutlined />
                                    <ItemTitle>Users & Groups</ItemTitle>
                                </Menu.Item>
                            )}
                            {showPolicies && (
                                <Menu.Item key="policies">
                                    <BankOutlined />
                                    <ItemTitle>Privileges</ItemTitle>
                                </Menu.Item>
                            )}
                        </Menu.ItemGroup>
                    )}
                    <Menu.ItemGroup title="Preferences">
                        <Menu.Item key="preferences">
                            <ToolOutlined />
                            <ItemTitle>Appearance</ItemTitle>
                        </Menu.Item>
                    </Menu.ItemGroup>
                </Menu>
            </SettingsBarContainer>
            <Switch>
                <Route exact path={path}>
                    <Redirect to={`${pathname}${pathname.endsWith('/') ? '' : '/'}${DEFAULT_PATH.path}`} />
                </Route>
                {PATHS.map((p) => (
                    <Route path={`${path}/${p.path.replace('/', '')}`} render={() => p.content} key={p.path} />
                ))}
            </Switch>
        </PageContainer>
    );
};
