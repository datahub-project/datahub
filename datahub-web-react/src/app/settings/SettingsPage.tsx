import {
    AppstoreOutlined,
    BankOutlined,
    BellOutlined,
    ControlOutlined,
    FilterOutlined,
    LoginOutlined,
    PushpinOutlined,
    QuestionCircleOutlined,
    SafetyCertificateOutlined,
    StarOutlined,
    TeamOutlined,
    ToolOutlined,
    UsergroupAddOutlined,
} from '@ant-design/icons';
import { Button, Divider, Menu, Typography } from 'antd';
import Cookies from 'js-cookie';
import React from 'react';
import { Redirect, Route, Switch, useHistory, useLocation, useRouteMatch } from 'react-router';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { isLoggedInVar } from '@app/auth/checkAuthStatus';
import { useUserContext } from '@app/context/useUserContext';
import { ManageOwnership } from '@app/entity/ownership/ManageOwnership';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { ManageViews } from '@app/entity/view/ManageViews';
import { ManageIdentities } from '@app/identity/ManageIdentities';
import { ManagePermissions } from '@app/permissions/ManagePermissions';
import { ManagePolicies } from '@app/permissions/policy/ManagePolicies';
import { AccessTokens } from '@app/settings/AccessTokens';
import { Preferences } from '@app/settings/Preferences';
import ManageHelpLink from '@app/settings/helpLink/ManageHelpLink';
import { ManageActorNotifications } from '@app/settings/personal/notifications/ManageActorNotifications';
import { useSubscriptionsEnabled } from '@app/settings/personal/notifications/utils';
import { ManageActorSubscriptions } from '@app/settings/personal/subscriptions/ManageActorSubscriptions';
import { PlatformIntegrations } from '@app/settings/platform/PlatformIntegrations';
import { PlatformSsoIntegrations } from '@app/settings/platform/PlatformSsoIntegrations';
import { PlatformNotifications } from '@app/settings/platform/notifications/PlatformNotifications';
import ManagePosts from '@app/settings/posts/ManagePosts';
import { useAppConfig } from '@app/useAppConfig';
import { useIsThemeV2 } from '@app/useIsThemeV2';
import { GlobalCfg } from '@src/conf';

const MenuItem = styled(Menu.Item)`
    display: flex;
    align-items: center;
`;

const PageContainer = styled.div`
    display: flex;
    overflow: auto;
    flex: 1;
    background-color: white;
    border-radius: 8px;
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

const ACRYL_PATHS = [
    { path: 'integrations', content: <PlatformIntegrations /> },
    { path: 'notifications', content: <PlatformNotifications /> },
    { path: 'sso', content: <PlatformSsoIntegrations /> },
    { path: 'personal-notifications', content: <ManageActorNotifications isPersonal canManageNotifications /> },
    { path: 'personal-subscriptions', content: <ManageActorSubscriptions isPersonal /> },
];
const menuStyle = { width: 256, marginTop: 8, overflow: 'hidden auto' };

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
    { path: 'policies', content: <ManagePolicies /> },
    { path: 'preferences', content: <Preferences /> },
    /* acryl-main only */
    ...ACRYL_PATHS,
    { path: 'permissions', content: <ManagePermissions /> },
    { path: 'views', content: <ManageViews /> },
    { path: 'ownership', content: <ManageOwnership /> },
    { path: 'posts', content: <ManagePosts /> },
    // { path: 'features', content: <Features /> }, OSS-Only Currently!
    { path: 'helpLink', content: <ManageHelpLink /> },
];

/**
 * The default selected path
 */
const DEFAULT_PATH = PATHS[0];

export const SettingsPage = () => {
    const subscriptionsEnabled = useSubscriptionsEnabled();
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
    const showGlobalSettings = me?.platformPrivileges?.manageGlobalSettings || false;
    const showViews = isViewsEnabled || false;
    const showOwnershipTypes = me && me?.platformPrivileges?.manageOwnershipTypes;
    const showHomePagePosts = me && me?.platformPrivileges?.manageGlobalAnnouncements && !readOnlyModeEnabled;
    const showFeatures = me?.platformPrivileges?.manageIngestion; // TODO: Add feature flag for this
    const showCustomHelpLink = me?.platformPrivileges?.manageGlobalSettings;
    const showAccessTokens = me && me?.platformPrivileges?.generatePersonalAccessTokens;

    const isThemeV2 = useIsThemeV2();

    const handleLogout = () => {
        analytics.event({ type: EventType.LogOutEvent });
        isLoggedInVar(false);
        Cookies.remove(GlobalCfg.CLIENT_AUTH_COOKIE);
        me.updateLocalState({ selectedViewUrn: undefined });
    };

    return (
        <PageContainer>
            <SettingsBarContainer>
                <SettingsBarHeader>
                    <PageTitle level={3}>Settings</PageTitle>
                    <Typography.Paragraph type="secondary">Manage your DataHub settings.</Typography.Paragraph>
                    {isThemeV2 && (
                        <Button
                            type="link"
                            href="/logOut"
                            onClick={handleLogout}
                            data-testid="log-out-menu-item"
                            style={{ padding: 0, margin: 0, height: 'auto', lineHeight: 'inherit' }}
                            danger
                        >
                            Sign Out
                        </Button>
                    )}
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
                    <Menu.ItemGroup title="Personal">
                        {showViews && (
                            <Menu.Item key="views">
                                <FilterOutlined />
                                <ItemTitle>My Views</ItemTitle>
                            </Menu.Item>
                        )}
                        {subscriptionsEnabled && (
                            <>
                                <Menu.Item key="personal-notifications">
                                    <BellOutlined />
                                    <ItemTitle>My Notifications</ItemTitle>
                                </Menu.Item>
                                <Menu.Item key="personal-subscriptions">
                                    <StarOutlined />
                                    <ItemTitle>My Subscriptions</ItemTitle>
                                </Menu.Item>
                            </>
                        )}
                    </Menu.ItemGroup>

                    {showAccessTokens ? (
                        <Menu.ItemGroup title="Developer">
                            <Menu.Item key="tokens">
                                <SafetyCertificateOutlined />
                                <ItemTitle>Access Tokens</ItemTitle>
                            </Menu.Item>
                        </Menu.ItemGroup>
                    ) : null}
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
                    {
                        /* acryl-main only */ showGlobalSettings && (
                            <Menu.ItemGroup title="Platform">
                                <Menu.Item key="sso">
                                    <LoginOutlined />
                                    <ItemTitle>SSO</ItemTitle>
                                </Menu.Item>
                                <Menu.Item key="integrations">
                                    <AppstoreOutlined />
                                    <ItemTitle>Integrations</ItemTitle>
                                </Menu.Item>
                                <Menu.Item key="notifications">
                                    <BellOutlined />
                                    <ItemTitle>Notifications</ItemTitle>
                                </Menu.Item>
                            </Menu.ItemGroup>
                        )
                    }

                    {(showViews || showOwnershipTypes || showHomePagePosts || showFeatures) && (
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
                            {showCustomHelpLink && (
                                <Menu.Item key="helpLink">
                                    <QuestionCircleOutlined /> <ItemTitle>Custom Help Link</ItemTitle>
                                </Menu.Item>
                            )}
                            {showHomePagePosts && (
                                <Menu.Item key="posts">
                                    <PushpinOutlined /> <ItemTitle>Home Page</ItemTitle>
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
