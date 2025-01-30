import React from 'react';
import { useHistory, useLocation, useRouteMatch, Redirect, Route, Switch } from 'react-router';
import { colors } from '@src/alchemy-components';
import { Button } from 'antd';
import {
    Bank,
    Bell,
    Funnel,
    House,
    ShieldCheck,
    Star,
    ToggleRight,
    Users,
    Wrench,
    UsersThree,
} from '@phosphor-icons/react';
import styled from 'styled-components';
import { useUserContext } from '../context/useUserContext';
import { PATHS, DEFAULT_PATH } from './settingsPaths';
import { NavBarMenuItems, NavBarMenuItemTypes } from '../homeV2/layout/navBarRedesign/types';
import NavBarMenu from '../homeV2/layout/navBarRedesign/NavBarMenu';
import { useIsThemeV2 } from '../useIsThemeV2';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';
import useGetLogoutHandler from '../auth/useGetLogoutHandler';
import { useAppConfig } from '../useAppConfig';

const PageContainer = styled.div`
    display: flex;
    overflow: auto;
    flex: 1;
    border-radius: ${(props) => props.theme.styles['border-radius-navbar-redesign']};
    gap: 16px;
    padding: 5px;
`;

const NavBarContainer = styled.div`
    padding: 20px 20px;
    background-color: white;
    display: flex;
    flex-direction: column;
    border-radius: ${(props) => props.theme.styles['border-radius-navbar-redesign']};
    box-shadow: ${(props) => props.theme.styles['box-shadow-navbar-redesign']};
    align-items: start;
    overflow: auto;
    width: 20%;
    min-width: 180px;
`;

const NavBarHeader = styled.div`
    margin-bottom: 4px;
    display: flex;
    width: 100%;
    align-items: center;
    justify-content: space-between;
`;

const NavBarTitle = styled.div`
    font-size: 16px;
    font-weight: 700;
    margin-bottom: 4px;
`;

const NavBarSubTitle = styled.div`
    font-size: 14px;
    color: ${colors.gray[1700]};
    margin-bottom: 8px;
`;

const NavBarMenuContainer = styled.div`
    padding-bottom: 8px; // Adds space below nav bar items on overflow.
    width: 100%;
`;

const ContentContainer = styled.div`
    border-radius: ${(props) => props.theme.styles['border-radius-navbar-redesign']};
    flex: 1;
    display: flex;
    overflow: auto;
    background-color: white;
    box-shadow: ${(props) => props.theme.styles['box-shadow-navbar-redesign']};
`;

export const SettingsPage = () => {
    const { path, url } = useRouteMatch();
    const { pathname } = useLocation();
    const history = useHistory();
    const subscriptionsEnabled = false;
    const me = useUserContext();
    const isThemeV2 = useIsThemeV2();
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const { config } = useAppConfig();

    const subRoutes = PATHS.map((p) => p.path.replace('/', ''));
    const currPathName = pathname.replace(path, '');
    const trimmedPathName = currPathName.endsWith('/') ? pathname.slice(0, pathname.length - 1) : currPathName;
    const splitPathName = trimmedPathName.split('/');
    const providedPath = splitPathName[1];
    const activePath = subRoutes.includes(providedPath) ? providedPath : DEFAULT_PATH.path.replace('/', '');

    const isViewsEnabled = config?.viewsConfig?.enabled;
    const isPoliciesEnabled = config?.policiesConfig?.enabled;
    const isIdentityManagementEnabled = config?.identityManagementConfig?.enabled;
    const { readOnlyModeEnabled } = config.featureFlags;

    const showViews = isViewsEnabled || false;
    const showPolicies = (isPoliciesEnabled && me && me?.platformPrivileges?.managePolicies) || false;
    const showUsersGroups = (isIdentityManagementEnabled && me && me?.platformPrivileges?.manageIdentities) || false;
    const showOwnershipTypes = me && me?.platformPrivileges?.manageOwnershipTypes;
    const showHomePagePosts = me && me?.platformPrivileges?.manageGlobalAnnouncements && !readOnlyModeEnabled;
    const showAccessTokens = me && me?.platformPrivileges?.generatePersonalAccessTokens;
    const showFeatures = me?.platformPrivileges?.manageIngestion; // TODO: Add feature flag for this

    // Menu Items based on PATHS
    const menuItems: NavBarMenuItems = {
        items: [
            // Personal Section
            {
                type: NavBarMenuItemTypes.Group,
                title: 'Personal',
                key: 'personal',
                items: [
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'My Views',
                        key: 'views',
                        link: `${url}/views`,
                        isHidden: !showViews,
                        icon: <Funnel />,
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'My Notifications',
                        key: 'personal-notifications',
                        link: `${url}/personal-notifications`,
                        isHidden: !subscriptionsEnabled,
                        icon: <Bell />,
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'My Subscriptions',
                        key: 'personal-subscriptions',
                        link: `${url}/personal-subscriptions`,
                        isHidden: !subscriptionsEnabled,
                        icon: <Star />,
                    },
                ],
            },
            // Developer Section
            {
                type: NavBarMenuItemTypes.Group,
                title: 'Developer',
                key: 'developer',
                items: [
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Access Tokens',
                        key: 'tokens',
                        link: `${url}/tokens`,
                        isHidden: !showAccessTokens,
                        icon: <ShieldCheck />,
                    },
                ],
            },
            // Access Section
            {
                type: NavBarMenuItemTypes.Group,
                title: 'Access',
                key: 'access',
                items: [
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Users & Groups',
                        key: 'identities',
                        link: `${url}/identities`,
                        isHidden: !showUsersGroups,
                        icon: <UsersThree />,
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Permissions',
                        key: 'permissions',
                        link: `${url}/permissions`,
                        isHidden: !showPolicies,
                        icon: <Bank />,
                    },
                ],
            },
            // Manage Section
            {
                type: NavBarMenuItemTypes.Group,
                title: 'Manage',
                key: 'manage',
                items: [
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Features',
                        key: 'features',
                        link: `${url}/features`,
                        isHidden: !showFeatures,
                        icon: <ToggleRight />,
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Home Page',
                        key: 'posts',
                        link: `${url}/posts`,
                        isHidden: !showHomePagePosts,
                        icon: <House />,
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Ownership Types',
                        key: 'ownership',
                        link: `${url}/ownership`,
                        isHidden: !showOwnershipTypes,
                        icon: <Users />,
                    },
                ],
            },
            // Preferences Section
            {
                type: NavBarMenuItemTypes.Group,
                title: 'Preferences',
                key: 'preferences',
                items: [
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Appearance',
                        key: 'preferences',
                        link: `${url}/preferences`,
                        icon: <Wrench />,
                    },
                ],
            },
        ],
    };

    const handleLogout = useGetLogoutHandler();

    return (
        <PageContainer>
            {/* Sidebar with NavBarMenu */}
            <NavBarContainer>
                <NavBarHeader>
                    <div>
                        <NavBarTitle>Settings</NavBarTitle>
                        <NavBarSubTitle>Manage your settings</NavBarSubTitle>
                    </div>
                    {isThemeV2 && !isShowNavBarRedesign && (
                        <Button href="/logOut" onClick={handleLogout} data-testid="log-out-menu-item" danger>
                            Log Out
                        </Button>
                    )}
                </NavBarHeader>
                <NavBarMenuContainer>
                    <NavBarMenu
                        isCollapsed={false}
                        selectedKey={activePath}
                        menu={menuItems}
                        iconSize={16}
                        onSelect={(key) => history.push(`${url}/${key}`)}
                    />
                </NavBarMenuContainer>
            </NavBarContainer>
            {/* Main Content */}
            <ContentContainer>
                <Switch>
                    <Route exact path={path}>
                        <Redirect to={`${pathname}${pathname.endsWith('/') ? '' : '/'}${DEFAULT_PATH.path}`} />
                    </Route>
                    {PATHS.map((p) => (
                        <Route path={`${path}/${p.path}`} key={p.path} render={() => p.content} />
                    ))}
                </Switch>
            </ContentContainer>
        </PageContainer>
    );
};
