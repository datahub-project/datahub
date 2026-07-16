import { ArrowLineLeft } from '@phosphor-icons/react/dist/csr/ArrowLineLeft';
import { ArrowLineRight } from '@phosphor-icons/react/dist/csr/ArrowLineRight';
import { Bank } from '@phosphor-icons/react/dist/csr/Bank';
import { Bell } from '@phosphor-icons/react/dist/csr/Bell';
import { Funnel } from '@phosphor-icons/react/dist/csr/Funnel';
import { House } from '@phosphor-icons/react/dist/csr/House';
import { ShieldCheck } from '@phosphor-icons/react/dist/csr/ShieldCheck';
import { Star } from '@phosphor-icons/react/dist/csr/Star';
import { ToggleRight } from '@phosphor-icons/react/dist/csr/ToggleRight';
import { Users } from '@phosphor-icons/react/dist/csr/Users';
import { UsersThree } from '@phosphor-icons/react/dist/csr/UsersThree';
import { Wrench } from '@phosphor-icons/react/dist/csr/Wrench';
import React, { Suspense, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Redirect, Route, Switch, useHistory, useLocation, useRouteMatch } from 'react-router';
import styled from 'styled-components';

import useGetLogoutHandler from '@app/auth/useGetLogoutHandler';
import { useUserContext } from '@app/context/useUserContext';
import NavBarMenu from '@app/homeV2/layout/navBarRedesign/NavBarMenu';
import { NavBarMenuItemTypes, NavBarMenuItems } from '@app/homeV2/layout/navBarRedesign/types';
import { DEFAULT_PATH, PATHS } from '@app/settingsV2/settingsPaths';
import { SuspenseBlock } from '@app/shared/SuspenseBlock';
import { useAppConfig } from '@app/useAppConfig';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { Button } from '@src/alchemy-components';

const COLLAPSED_NAV_WIDTH = 72;

const PageContainer = styled.div`
    display: flex;
    overflow: auto;
    flex: 1;
    border-radius: ${(props) => props.theme.styles['border-radius-navbar-redesign']};
    gap: 16px;
    padding: 5px;
`;

const NavBarContainer = styled.div<{ $isCollapsed: boolean }>`
    box-sizing: border-box;
    padding: ${(props) => (props.$isCollapsed ? '16px 12px' : '20px 20px')};
    background-color: ${(props) => props.theme.colors.bg};
    display: flex;
    flex-direction: column;
    border-radius: ${(props) => props.theme.styles['border-radius-navbar-redesign']};
    box-shadow: ${(props) => props.theme.colors.shadowSm};
    align-items: ${(props) => (props.$isCollapsed ? 'center' : 'start')};
    overflow: auto;
    width: ${(props) => (props.$isCollapsed ? `${COLLAPSED_NAV_WIDTH}px` : '20%')};
    min-width: ${(props) => (props.$isCollapsed ? `${COLLAPSED_NAV_WIDTH}px` : '180px')};
    transition:
        width 180ms ease-out,
        min-width 180ms ease-out,
        padding 180ms ease-out;
`;

const NavBarHeader = styled.div`
    margin-bottom: 4px;
    display: flex;
    width: 100%;
    align-items: center;
    justify-content: space-between;
`;

const NavBarHeaderMain = styled.div<{ $isCollapsed: boolean }>`
    display: ${(props) => (props.$isCollapsed ? 'none' : 'flex')};
    flex-direction: column;
`;

const NavBarTitle = styled.div`
    font-size: 16px;
    font-weight: 700;
    color: ${(props) => props.theme.colors.text};
    margin-bottom: 0;
`;

const NavBarSubTitle = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textSecondary};
    margin-bottom: 8px;
`;

const NavBarHeaderActions = styled.div<{ $isCollapsed: boolean }>`
    display: flex;
    align-items: center;
    gap: 8px;
    width: ${(props) => (props.$isCollapsed ? '100%' : 'auto')};
    justify-content: ${(props) => (props.$isCollapsed ? 'center' : 'flex-end')};
`;

const NavBarDivider = styled.div`
    width: 100%;
    height: 1px;
    margin: 4px 0 12px 0;
    background-color: ${(props) => props.theme.colors.border};
`;

const NavBarMenuContainer = styled.div<{ $isCollapsed: boolean }>`
    padding-bottom: ${(props) => (props.$isCollapsed ? '0' : '8px')}; // Adds space below nav bar items on overflow.
    width: 100%;
`;

const ContentContainer = styled.div`
    border-radius: ${(props) => props.theme.styles['border-radius-navbar-redesign']};
    flex: 1;
    display: flex;
    overflow: auto;
    background-color: ${(props) => props.theme.colors.bg};
    box-shadow: ${(props) => props.theme.colors.shadowSm};
`;

const fillIcon = (icon: React.ReactElement) => React.cloneElement(icon, { weight: 'fill' });

const SettingsPageContent = () => {
    const { t } = useTranslation('settings.page');
    const { path, url } = useRouteMatch();
    const { pathname } = useLocation();
    const history = useHistory();
    const [isCollapsed, setIsCollapsed] = useState(false);
    const subscriptionsEnabled = false;
    const me = useUserContext();
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
                title: t('nav.personal'),
                key: 'personal',
                items: [
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: t('nav.views'),
                        key: 'views',
                        link: `${url}/views`,
                        isHidden: !showViews,
                        icon: <Funnel />,
                        selectedIcon: fillIcon(<Funnel />),
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: t('nav.myNotifications'),
                        key: 'personal-notifications',
                        link: `${url}/personal-notifications`,
                        isHidden: !subscriptionsEnabled,
                        icon: <Bell />,
                        selectedIcon: fillIcon(<Bell />),
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: t('nav.mySubscriptions'),
                        key: 'personal-subscriptions',
                        link: `${url}/personal-subscriptions`,
                        isHidden: !subscriptionsEnabled,
                        icon: <Star />,
                        selectedIcon: fillIcon(<Star />),
                    },
                ],
            },
            // Developer Section
            {
                type: NavBarMenuItemTypes.Group,
                title: t('nav.developer'),
                key: 'developer',
                items: [
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: t('nav.accessTokens'),
                        key: 'tokens',
                        link: `${url}/tokens`,
                        isHidden: !showAccessTokens,
                        icon: <ShieldCheck />,
                        selectedIcon: fillIcon(<ShieldCheck />),
                    },
                ],
            },
            // Access Section
            {
                type: NavBarMenuItemTypes.Group,
                title: t('nav.access'),
                key: 'access',
                items: [
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: t('nav.usersGroups'),
                        key: 'identities',
                        link: `${url}/identities`,
                        isHidden: !showUsersGroups,
                        icon: <UsersThree />,
                        selectedIcon: fillIcon(<UsersThree />),
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: t('nav.permissions'),
                        key: 'permissions',
                        link: `${url}/permissions`,
                        isHidden: !showPolicies,
                        icon: <Bank />,
                        selectedIcon: fillIcon(<Bank />),
                    },
                ],
            },
            // Manage Section
            {
                type: NavBarMenuItemTypes.Group,
                title: t('nav.manage'),
                key: 'manage',
                items: [
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: t('nav.features'),
                        key: 'features',
                        link: `${url}/features`,
                        isHidden: !showFeatures,
                        icon: <ToggleRight />,
                        selectedIcon: fillIcon(<ToggleRight />),
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: t('nav.homePage'),
                        key: 'posts',
                        link: `${url}/posts`,
                        isHidden: !showHomePagePosts,
                        icon: <House />,
                        selectedIcon: fillIcon(<House />),
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: t('nav.ownershipTypes'),
                        key: 'ownership',
                        link: `${url}/ownership`,
                        isHidden: !showOwnershipTypes,
                        icon: <Users />,
                        selectedIcon: fillIcon(<Users />),
                    },
                ],
            },
            // Preferences Section
            {
                type: NavBarMenuItemTypes.Group,
                title: t('nav.preferences'),
                key: 'preferences',
                items: [
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: t('nav.appearance'),
                        key: 'preferences',
                        link: `${url}/preferences`,
                        icon: <Wrench />,
                        selectedIcon: fillIcon(<Wrench />),
                    },
                ],
            },
        ],
    };

    const handleLogout = useGetLogoutHandler();
    const handleToggleCollapse = () => {
        setIsCollapsed((current) => !current);
    };
    const toggleSidebarLabel = isCollapsed ? t('nav.expandSidebar') : t('nav.collapseSidebar');

    return (
        <PageContainer>
            {/* Sidebar with NavBarMenu */}
            <NavBarContainer $isCollapsed={isCollapsed}>
                <NavBarHeader>
                    <NavBarHeaderMain $isCollapsed={isCollapsed}>
                        <NavBarTitle>{t('title')}</NavBarTitle>
                        <NavBarSubTitle>{t('subTitle')}</NavBarSubTitle>
                    </NavBarHeaderMain>
                    <NavBarHeaderActions $isCollapsed={isCollapsed}>
                        {!isShowNavBarRedesign && !isCollapsed && (
                            <a href="/logOut">
                                <Button
                                    variant="outline"
                                    color="red"
                                    onClick={handleLogout}
                                    data-testid="log-out-menu-item"
                                >
                                    {t('logOut')}
                                </Button>
                            </a>
                        )}
                        <Button
                            variant="text"
                            color="gray"
                            size="lg"
                            isCircle
                            type="button"
                            onClick={handleToggleCollapse}
                            aria-label={toggleSidebarLabel}
                            title={toggleSidebarLabel}
                            icon={{ icon: isCollapsed ? ArrowLineRight : ArrowLineLeft }}
                        />
                    </NavBarHeaderActions>
                </NavBarHeader>
                <NavBarDivider />
                <NavBarMenuContainer $isCollapsed={isCollapsed}>
                    <NavBarMenu
                        isCollapsed={isCollapsed}
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
                        <Route
                            path={`${path}/${p.path}`}
                            key={p.path}
                            render={() => <Suspense fallback={<SuspenseBlock />}>{p.content}</Suspense>}
                        />
                    ))}
                </Switch>
            </ContentContainer>
        </PageContainer>
    );
};

export const SettingsPage = () => {
    return (
        <Suspense fallback={<SuspenseBlock />}>
            <SettingsPageContent />
        </Suspense>
    );
};
