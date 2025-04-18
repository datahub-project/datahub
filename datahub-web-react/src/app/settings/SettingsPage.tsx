import {
    BankOutlined,
    ControlOutlined,
    FilterOutlined,
    PushpinOutlined,
    SafetyCertificateOutlined,
    TeamOutlined,
    ToolOutlined,
    UsergroupAddOutlined,
} from '@ant-design/icons';
import { Divider, Menu, Typography } from 'antd';
import React from 'react';
import { Redirect, Route, Switch, useHistory, useLocation, useRouteMatch } from 'react-router';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { ManageOwnership } from '@app/entity/ownership/ManageOwnership';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { ManageViews } from '@app/entity/view/ManageViews';
import { ManageIdentities } from '@app/identity/ManageIdentities';
import { ManagePermissions } from '@app/permissions/ManagePermissions';
import { AccessTokens } from '@app/settings/AccessTokens';
import { Preferences } from '@app/settings/Preferences';
import { Features } from '@app/settings/features/Features';
import ManagePosts from '@app/settings/posts/ManagePosts';
import { useAppConfig } from '@app/useAppConfig';

const MenuItem = styled(Menu.Item)`
    display: flex;
    align-items: center;
`;

const PageContainer = styled.div`
    display: flex;
    overflow: auto;
    flex: 1;
`;

const SettingsBarContainer = styled.div`
    padding-top: 20px;
    border-right: 1px solid ${ANTD_GRAY[5]};
    display: flex;
    flex-direction: column;
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

const menuStyle = { width: 256, 'margin-top': 8, overflow: 'hidden auto' };

const NewTag = styled.span`
    padding: 4px 8px;
    margin-left: 8px;

    border-radius: 24px;
    background: #f1fbfe;

    color: #09739a;
    font-size: 12px;
`;

/**
 * URL Paths for each settings page.
 */
const PATHS = [
    { path: 'tokens', content: <AccessTokens /> },
    { path: 'identities', content: <ManageIdentities version="v1" /> },
    { path: 'permissions', content: <ManagePermissions /> },
    { path: 'preferences', content: <Preferences /> },
    { path: 'views', content: <ManageViews /> },
    { path: 'ownership', content: <ManageOwnership /> },
    { path: 'posts', content: <ManagePosts /> },
    { path: 'features', content: <Features /> },
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

    const me = useUserContext();
    const { config } = useAppConfig();

    const isPoliciesEnabled = config?.policiesConfig?.enabled;
    const isIdentityManagementEnabled = config?.identityManagementConfig?.enabled;
    const isViewsEnabled = config?.viewsConfig?.enabled;
    const { readOnlyModeEnabled } = config.featureFlags;

    const showPolicies = (isPoliciesEnabled && me && me?.platformPrivileges?.managePolicies) || false;
    const showUsersGroups = (isIdentityManagementEnabled && me && me?.platformPrivileges?.manageIdentities) || false;
    const showViews = isViewsEnabled || false;
    const showOwnershipTypes = me && me?.platformPrivileges?.manageOwnershipTypes;
    const showHomePagePosts = me && me?.platformPrivileges?.manageGlobalAnnouncements && !readOnlyModeEnabled;
    const showFeatures = me?.platformPrivileges?.manageIngestion; // TODO: Add feature flag for this

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
                    style={menuStyle}
                    selectedKeys={[activePath]}
                    onClick={(newPath) => {
                        history.replace(`${url}/${newPath.key}`);
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
                                <Menu.Item key="permissions">
                                    <BankOutlined />
                                    <ItemTitle>Permissions</ItemTitle>
                                </Menu.Item>
                            )}
                        </Menu.ItemGroup>
                    )}
                    {(showViews || showOwnershipTypes || showHomePagePosts) && (
                        <Menu.ItemGroup title="Manage">
                            {showFeatures && (
                                <MenuItem key="features">
                                    <ControlOutlined />
                                    <ItemTitle>Features</ItemTitle>
                                    <NewTag>New!</NewTag>
                                </MenuItem>
                            )}
                            {showViews && (
                                <Menu.Item key="views">
                                    <FilterOutlined /> <ItemTitle>My Views</ItemTitle>
                                </Menu.Item>
                            )}
                            {showOwnershipTypes && (
                                <Menu.Item key="ownership">
                                    <TeamOutlined /> <ItemTitle>Ownership Types</ItemTitle>
                                </Menu.Item>
                            )}
                            {showHomePagePosts && (
                                <Menu.Item key="posts">
                                    <PushpinOutlined /> <ItemTitle>Home Page Posts</ItemTitle>
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
