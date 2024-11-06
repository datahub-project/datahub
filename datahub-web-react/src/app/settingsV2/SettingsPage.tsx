import React from 'react';
import { Menu, Typography, Divider, Button } from 'antd';
import {
    BankOutlined,
    SafetyCertificateOutlined,
    UsergroupAddOutlined,
    AppstoreOutlined,
    BellOutlined,
    LoginOutlined,
    ToolOutlined,
    FilterOutlined,
    TeamOutlined,
    StarOutlined,
    PushpinOutlined,
    QuestionCircleOutlined,
} from '@ant-design/icons';
import { Redirect, Route, useHistory, useLocation, useRouteMatch, Switch } from 'react-router';
import styled from 'styled-components';
import Cookies from 'js-cookie';
import { ANTD_GRAY } from '../entity/shared/constants';
import { ManageIdentities } from '../identity/ManageIdentities';
import { ManagePermissions } from '../permissions/ManagePermissions';
import { useAppConfig } from '../useAppConfig';
import { AccessTokens } from './AccessTokens';
import { PlatformIntegrations } from './platform/PlatformIntegrations';
import { PlatformNotifications } from './platform/notifications/PlatformNotifications';
import { PlatformSsoIntegrations } from './platform/PlatformSsoIntegrations';
import { Preferences } from './Preferences';
import { ManagePolicies } from '../permissions/policy/ManagePolicies';
import { ManageViews } from '../entity/view/ManageViews';
import { useUserContext } from '../context/useUserContext';
import { ManageOwnership } from '../entity/ownership/ManageOwnership';
import { ManageActorNotifications } from './personal/notifications/ManageActorNotifications';
import { ManageActorSubscriptions } from './personal/subscriptions/ManageActorSubscriptions';
import { useSubscriptionsEnabled } from './personal/notifications/utils';
import ManagePosts from './posts/ManagePosts';
import ManageHelpLink from './helpLink/ManageHelpLink';

import analytics, { EventType } from '../analytics';
import { GlobalCfg } from '../../conf';
import { isLoggedInVar } from '../auth/checkAuthStatus';
import { useIsThemeV2 } from '../useIsThemeV2';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';

const PageContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    display: flex;
    overflow: auto;
    flex: 1;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    ${(props) => !props.$isShowNavBarRedesign && 'background-color: white;'}
    gap: ${(props) => (props.$isShowNavBarRedesign ? '16px' : '0')};
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        padding: 5px;
    `}
`;

const SettingsBarContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    padding-top: 20px;
    ${(props) => !props.$isShowNavBarRedesign && `border-right: 1px solid ${ANTD_GRAY[5]};`}
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        border-radius: ${props.theme.styles['border-radius-navbar-redesign']};
        background-color: white;
    `}
    display: flex;
    flex-direction: column;
    ${(props) => props.$isShowNavBarRedesign && `box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};`}
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

const SettingsContentContainer = styled.div`
    border-radius: ${(props) => props.theme.styles['border-radius-navbar-redesign']};
    width: 100%;
    display: flex;
    overflow: auto;
    background-color: white;
    box-shadow: ${(props) => props.theme.styles['box-shadow-navbar-redesign']};
`;

const StyledMenu = styled(Menu)<{ $isShowNavBarRedesign?: boolean }>`
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `border-radius: 0 0 ${props.theme.styles['border-radius-navbar-redesign']} ${props.theme.styles['border-radius-navbar-redesign']};`}
`;

const ACRYL_PATHS = [
    { path: 'integrations', content: <PlatformIntegrations /> },
    { path: 'notifications', content: <PlatformNotifications /> },
    { path: 'sso', content: <PlatformSsoIntegrations /> },
    { path: 'personal-notifications', content: <ManageActorNotifications isPersonal /> },
    { path: 'personal-subscriptions', content: <ManageActorSubscriptions isPersonal /> },
];
const menuStyle = { width: 256, marginTop: 8, overflow: 'hidden auto' };

/**
 * URL Paths for each settings page.
 */
const PATHS = [
    { path: 'tokens', content: <AccessTokens /> },
    { path: 'identities', content: <ManageIdentities /> },
    { path: 'policies', content: <ManagePolicies /> },
    { path: 'preferences', content: <Preferences /> },
    /* acryl-main only */
    ...ACRYL_PATHS,
    { path: 'permissions', content: <ManagePermissions /> },
    { path: 'views', content: <ManageViews /> },
    { path: 'ownership', content: <ManageOwnership /> },
    { path: 'posts', content: <ManagePosts /> },
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

    const isPoliciesEnabled = config?.policiesConfig.enabled;
    const isIdentityManagementEnabled = config?.identityManagementConfig.enabled;
    const isViewsEnabled = config?.viewsConfig.enabled;
    const { readOnlyModeEnabled } = config.featureFlags;

    const showPolicies = (isPoliciesEnabled && me && me?.platformPrivileges?.managePolicies) || false;
    const showUsersGroups = (isIdentityManagementEnabled && me && me?.platformPrivileges?.manageIdentities) || false;
    const showGlobalSettings = me?.platformPrivileges?.manageGlobalSettings || false;
    const showViews = isViewsEnabled || false;
    const showOwnershipTypes = me && me?.platformPrivileges?.manageOwnershipTypes;
    const showHomePagePosts = me && me?.platformPrivileges?.manageGlobalAnnouncements && !readOnlyModeEnabled;
    const showCustomHelpLink = me?.platformPrivileges?.manageGlobalSettings;
    const showAccessTokens = me && me?.platformPrivileges?.generatePersonalAccessTokens;

    const isThemeV2 = useIsThemeV2();
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const handleLogout = () => {
        analytics.event({ type: EventType.LogOutEvent });
        isLoggedInVar(false);
        Cookies.remove(GlobalCfg.CLIENT_AUTH_COOKIE);
        me.updateLocalState({ selectedViewUrn: undefined });
    };

    const FinalSettingsContentContainer = isShowNavBarRedesign ? SettingsContentContainer : React.Fragment;

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            <SettingsBarContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
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
                <StyledMenu
                    selectable={false}
                    mode="inline"
                    style={menuStyle}
                    selectedKeys={[activePath]}
                    $isShowNavBarRedesign={isShowNavBarRedesign}
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

                    {(showOwnershipTypes || showHomePagePosts || showCustomHelpLink) && (
                        <Menu.ItemGroup title="Manage">
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
                </StyledMenu>
            </SettingsBarContainer>
            <Switch>
                <FinalSettingsContentContainer>
                    <Route exact path={path}>
                        <Redirect to={`${pathname}${pathname.endsWith('/') ? '' : '/'}${DEFAULT_PATH.path}`} />
                    </Route>
                    {PATHS.map((p) => (
                        <Route path={`${path}/${p.path.replace('/', '')}`} render={() => p.content} key={p.path} />
                    ))}
                </FinalSettingsContentContainer>
            </Switch>
        </PageContainer>
    );
};
