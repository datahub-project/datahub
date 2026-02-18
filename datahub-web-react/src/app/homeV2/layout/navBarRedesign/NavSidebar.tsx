import {
    AppWindow,
    BookBookmark,
    FileText,
    Gear,
    Globe,
    HardDrives,
    Plugs,
    Question,
    SignOut,
    SquaresFour,
    Tag,
    TextColumns,
    TrendUp,
    UserCircle,
} from '@phosphor-icons/react';
import React, { useContext, useEffect } from 'react';
import { useLocation } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { useNavBarContext } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
import NavBarHeader from '@app/homeV2/layout/navBarRedesign/NavBarHeader';
import NavBarMenu from '@app/homeV2/layout/navBarRedesign/NavBarMenu';
import NavSkeleton from '@app/homeV2/layout/navBarRedesign/NavBarSkeleton';
import {
    NavBarMenuDropdownItem,
    NavBarMenuDropdownItemElement,
    NavBarMenuGroup,
    NavBarMenuItemTypes,
    NavBarMenuItems,
} from '@app/homeV2/layout/navBarRedesign/types';
import useSelectedKey from '@app/homeV2/layout/navBarRedesign/useSelectedKey';
import { useShowHomePageRedesign } from '@app/homeV3/context/hooks/useShowHomePageRedesign';
import { useMFEConfigFromBackend } from '@app/mfeframework/mfeConfigLoader';
import { getMfeMenuDropdownItems, getMfeMenuItems } from '@app/mfeframework/mfeNavBarMenuUtils';
import OnboardingContext from '@app/onboarding/OnboardingContext';
import { useOnboardingTour } from '@app/onboarding/OnboardingTourContext.hooks';
import { useIsHomePage } from '@app/shared/useIsHomePage';
import { useGetIngestionLink } from '@app/sharedV2/ingestionSources/useGetIngestionLink';
import { useHasIngestionSources } from '@app/sharedV2/ingestionSources/useHasIngestionSources';
import { useAppConfig, useBusinessAttributesFlag, useIsContextDocumentsEnabled } from '@app/useAppConfig';
import { colors } from '@src/alchemy-components';
import { getColor } from '@src/alchemy-components/theme/utils';
import useGetLogoutHandler from '@src/app/auth/useGetLogoutHandler';
import { HOME_PAGE_INGESTION_ID } from '@src/app/onboarding/config/HomePageOnboardingConfig';
import { useHandleOnboardingTour } from '@src/app/onboarding/useHandleOnboardingTour';
import { useUpdateEducationStepsAllowList } from '@src/app/onboarding/useUpdateEducationStepsAllowList';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { HelpLinkRoutes, PageRoutes } from '@src/conf/Global';
import { EntityType } from '@src/types.generated';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

import AcrylIcon from '@images/acryl-light-mark.svg?react';

const Container = styled.div`
    height: 100vh;
    background-color: ${colors.gray[1600]};
    display: flex;
    flex: column;
    align-items: center;
`;

const Content = styled.div<{ isCollapsed: boolean }>`
    display: flex;
    flex-direction: column;
    height: 100%;
    width: ${(props) => (props.isCollapsed ? '60px' : '264px')};
    transition: width 250ms ease-in-out;
    overflow-x: hidden;
`;

const Header = styled.div`
    padding: 17px 8px 8px 16px;
    border-bottom: 1px solid ${colors.gray[100]};
`;

const ScrollableContent = styled.div`
    display: flex;
    flex-direction: column;
    padding: 0px 8px 0px 16px;
    flex: 1;
    overflow-y: auto;
    overflow-x: hidden;
    min-height: 0;

    /* Custom scrollbar styling */
    &::-webkit-scrollbar {
        width: 6px;
    }

    &::-webkit-scrollbar-track {
        background: transparent;
    }

    &::-webkit-scrollbar-thumb {
        background: #a9adbd;
        border-radius: 3px;
    }

    &::-webkit-scrollbar-thumb:hover {
        background: #81879f;
    }

    scrollbar-width: thin;
    scrollbar-color: #a9adbd transparent;
`;

const Footer = styled.div`
    padding: 8px 8px 17px 16px;
    border-top: 1px solid ${colors.gray[100]};
`;

const CustomLogo = styled.img`
    object-fit: contain;
    max-height: 26px;
    max-width: 26px;
    min-height: 20px;
    min-width: 20px;
`;

const DEFAULT_LOGO = 'assets/logos/acryl-dark-mark.svg';

const MenuWrapper = styled.div`
    margin-top: 14px;
    display: flex;
    flex-direction: column;
`;

export const NavSidebar = () => {
    const entityRegistry = useEntityRegistry();
    const themeConfig = useTheme();

    const { toggle, isCollapsed, selectedKey, setSelectedKey } = useNavBarContext();
    const appConfig = useAppConfig();
    const userContext = useUserContext();
    const me = useUserContext();
    const isHomePage = useIsHomePage();
    const location = useLocation();
    const showHomepageRedesign = useShowHomePageRedesign();
    const isContextDocumentsEnabled = useIsContextDocumentsEnabled();

    const { isUserInitializing } = useContext(OnboardingContext);
    const { triggerModalTour } = useOnboardingTour();
    const { showOnboardingTour } = useHandleOnboardingTour();
    const { config } = useAppConfig();
    const logout = useGetLogoutHandler();

    const { hasIngestionSources } = useHasIngestionSources();
    const ingestionLink = useGetIngestionLink(hasIngestionSources);

    const showAnalytics = (config?.analyticsConfig?.enabled && me && me?.platformPrivileges?.viewAnalytics) || false;
    const showStructuredProperties =
        config?.featureFlags?.showManageStructuredProperties &&
        (me.platformPrivileges?.manageStructuredProperties || me.platformPrivileges?.viewStructuredPropertiesPage);
    const showManageTags =
        config?.featureFlags?.showManageTags &&
        (me.platformPrivileges?.manageTags || me.platformPrivileges?.viewManageTags);
    const businessAttributesFlag = useBusinessAttributesFlag();

    const showDataSources =
        config.managedIngestionConfig.enabled &&
        me &&
        me.platformPrivileges?.manageIngestion &&
        me.platformPrivileges?.manageSecrets;

    // Update education steps allow list
    useUpdateEducationStepsAllowList(!!showDataSources, HOME_PAGE_INGESTION_ID);

    const customLogoUrl = appConfig.config.visualConfig.logoUrl;
    const hasCustomLogo = customLogoUrl && customLogoUrl !== DEFAULT_LOGO;
    const logoComponent = hasCustomLogo ? <CustomLogo alt="logo" src={customLogoUrl} /> : <AcrylIcon />;

    const HelpContentMenuItems = themeConfig.content.menu.items.map((value) => ({
        title: value.label,
        description: value.description || '',
        link: value.path,
        isHidden: false,
        isExternalLink: true,
        key: `helpMenu${value.label}`,
    })) as NavBarMenuDropdownItemElement[];

    // --- MFE YAML CONFIG ---
    const mfeConfig: any = useMFEConfigFromBackend();

    // MFE section (dropdown or spread)
    let mfeSection: any[] = [];
    if (mfeConfig) {
        if (mfeConfig.subNavigationMode) {
            mfeSection = [
                {
                    type: NavBarMenuItemTypes.Dropdown,
                    title: 'MFE Apps',
                    icon: <AppWindow />,
                    key: 'mfe-dropdown',
                    items: getMfeMenuDropdownItems(mfeConfig),
                } as NavBarMenuDropdownItem,
            ];
        } else {
            mfeSection = [
                {
                    type: NavBarMenuItemTypes.Group,
                    key: 'mfe-group',
                    title: 'MFE Apps',
                    items: getMfeMenuItems(mfeConfig),
                } as NavBarMenuGroup,
            ];
        }
    }
    function handleHomeclick() {
        if (isHomePage && showHomepageRedesign) {
            toggle();
        }
    }

    const headerMenu: NavBarMenuItems = {
        items: [
            {
                type: NavBarMenuItemTypes.Item,
                title: 'Home',
                icon: <SquaresFour />,
                selectedIcon: <SquaresFour weight="fill" />,
                key: 'home',
                link: PageRoutes.ROOT,
                onlyExactPathMapping: true,
                onClick: () => handleHomeclick(),
                dataTestId: 'nav-bar-item-home',
            },
        ],
    };

    const mainContentMenu: NavBarMenuItems = {
        items: [
            ...mfeSection,
            {
                type: NavBarMenuItemTypes.Group,
                key: 'context',
                title: 'Context',
                isHidden: !isContextDocumentsEnabled,
                items: [
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Documents',
                        key: 'contextDocuments',
                        icon: <FileText />,
                        selectedIcon: <FileText weight="fill" />,
                        link: PageRoutes.CONTEXT_DOCUMENTS,
                        additionalLinksForPathMatching: [`/${entityRegistry.getPathName(EntityType.Document)}/:urn`],
                        badge: {
                            label: 'BETA',
                            show: true,
                            showDot: false,
                        },
                    },
                ],
            },
            {
                type: NavBarMenuItemTypes.Group,
                key: 'govern',
                title: 'Govern',
                items: [
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Glossary',
                        key: 'glossary',
                        icon: <BookBookmark />,
                        selectedIcon: <BookBookmark weight="fill" />,
                        link: PageRoutes.GLOSSARY,
                        additionalLinksForPathMatching: entityRegistry
                            .getGlossaryEntities()
                            .map((entity) => `/${entity.getPathName()}/:urn`),
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Tags',
                        key: 'tag',
                        icon: <Tag />,
                        selectedIcon: <Tag weight="fill" />,
                        link: PageRoutes.MANAGE_TAGS,
                        isHidden: !showManageTags,
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Business Attributes',
                        key: 'businessAttributes',
                        icon: <HardDrives />,
                        selectedIcon: <HardDrives weight="fill" />,
                        link: PageRoutes.BUSINESS_ATTRIBUTE,
                        isHidden: !businessAttributesFlag,
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Applications',
                        key: 'applications',
                        icon: <AppWindow />,
                        selectedIcon: <AppWindow weight="fill" />,
                        link: PageRoutes.MANAGE_APPLICATIONS,
                        isHidden: !(appConfig.config.visualConfig.application?.showApplicationInNavigation ?? false),
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Domains',
                        key: 'domains',
                        icon: <Globe />,
                        selectedIcon: <Globe weight="fill" />,
                        link: PageRoutes.DOMAINS,
                        additionalLinksForPathMatching: [`/${entityRegistry.getPathName(EntityType.Domain)}/:urn`],
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Structured Properties',
                        key: 'structuredProperties',
                        isHidden: !showStructuredProperties,
                        icon: <TextColumns />,
                        selectedIcon: <TextColumns weight="fill" />,
                        link: PageRoutes.STRUCTURED_PROPERTIES,
                    },
                ],
            },
            {
                type: NavBarMenuItemTypes.Group,
                key: 'admin',
                title: 'Admin',
                items: [
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Data Sources',
                        key: 'dataSources',
                        isHidden: !showDataSources,
                        icon: <Plugs />,
                        selectedIcon: <Plugs weight="fill" />,
                        link: ingestionLink,
                        onClick: () => {
                            if (ingestionLink === PageRoutes.INGESTION_CREATE) {
                                analytics.event({
                                    type: EventType.EnterIngestionFlowEvent,
                                    entryPoint: 'nav_menu',
                                });
                            }
                        },
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Analytics',
                        icon: <TrendUp />,
                        selectedIcon: <TrendUp weight="fill" />,
                        key: 'analytics',
                        isHidden: !showAnalytics,
                        link: PageRoutes.ANALYTICS,
                    },
                ],
            },
        ],
    };

    const footerMenu: NavBarMenuItems = {
        items: [
            {
                type: NavBarMenuItemTypes.Item,
                title: 'Profile',
                icon: <UserCircle />,
                selectedIcon: <UserCircle weight="fill" />,
                key: 'profile',
                link: `/${entityRegistry.getPathName(EntityType.CorpUser)}/${userContext.urn}`,
            },
            {
                type: NavBarMenuItemTypes.Item,
                title: 'Settings',
                icon: <Gear />,
                selectedIcon: <Gear weight="fill" />,
                key: 'settings',
                link: '/settings',
            },
            {
                type: NavBarMenuItemTypes.Dropdown,
                title: 'Help',
                icon: <Question />,
                selectedIcon: <Question weight="fill" />,
                key: 'help',
                items: [
                    {
                        type: NavBarMenuItemTypes.DropdownElement,
                        title: 'Product Tour',
                        description: 'Take a quick tour of this page',
                        key: 'helpProductTour',
                        onClick: () => {
                            if (isHomePage) {
                                triggerModalTour();
                            } else {
                                // Track Product Tour button click for non-home pages
                                analytics.event({
                                    type: EventType.ProductTourButtonClickEvent,
                                    originPage: location.pathname,
                                });
                                showOnboardingTour();
                            }
                        },
                    },
                    {
                        type: NavBarMenuItemTypes.DropdownElement,
                        title: 'GraphQL',
                        description: 'Explore the GraphQL API',
                        link: resolveRuntimePath(HelpLinkRoutes.GRAPHIQL),
                        isExternalLink: true,
                        key: 'helpGraphQL',
                    },
                    {
                        type: NavBarMenuItemTypes.DropdownElement,
                        title: 'OpenAPI',
                        description: 'Explore the OpenAPI endpoints',
                        link: resolveRuntimePath(HelpLinkRoutes.OPENAPI),
                        isExternalLink: true,
                        key: 'helpOpenAPI',
                    },
                    ...HelpContentMenuItems,
                    {
                        type: NavBarMenuItemTypes.DropdownElement,
                        title: config?.appVersion || '',
                        isHidden: !config?.appVersion,
                        isExternalLink: true,
                        key: 'helpAppVersion',
                        disabled: true,
                    },
                ],
            },
            {
                type: NavBarMenuItemTypes.Item,
                title: 'Sign out',
                icon: <SignOut data-testid="log-out-menu-item" />,
                key: 'signOut',
                onClick: logout,
                href: resolveRuntimePath('/logOut'),
                dataTestId: 'nav-sidebar-sign-out',
            },
        ],
    };

    // Combine all menus for selected key calculation
    const allMenuItems: NavBarMenuItems = {
        items: [...headerMenu.items, ...mainContentMenu.items, ...footerMenu.items],
    };
    const sk = useSelectedKey(allMenuItems);

    useEffect(() => setSelectedKey(sk), [sk, setSelectedKey]);

    const showSkeleton = isUserInitializing || !appConfig.loaded || !userContext.loaded;

    const renderSvgSelectedGradientForReusingInIcons = () => {
        return (
            <svg
                style={{ width: 0, height: 0, position: 'absolute', visibility: 'hidden' }}
                aria-hidden="true"
                focusable="false"
            >
                <linearGradient id="menu-item-selected-gradient" x2="1" y2="1">
                    <stop offset="1%" stopColor={getColor('primary', 300, themeConfig)} />
                    <stop offset="99%" stopColor={getColor('primary', 500, themeConfig)} />
                </linearGradient>
            </svg>
        );
    };

    return (
        <Container>
            {renderSvgSelectedGradientForReusingInIcons()}
            <Content id="nav-sidebar" data-collapsed={isCollapsed} isCollapsed={isCollapsed}>
                {showSkeleton ? (
                    <NavSkeleton isCollapsed={isCollapsed} />
                ) : (
                    <>
                        <Header>
                            <NavBarHeader logotype={logoComponent} />
                            <MenuWrapper>
                                <NavBarMenu selectedKey={selectedKey} isCollapsed={isCollapsed} menu={headerMenu} />
                            </MenuWrapper>
                        </Header>
                        <ScrollableContent>
                            <MenuWrapper>
                                <NavBarMenu
                                    selectedKey={selectedKey}
                                    isCollapsed={isCollapsed}
                                    menu={mainContentMenu}
                                />
                            </MenuWrapper>
                        </ScrollableContent>
                        <Footer>
                            <NavBarMenu selectedKey={selectedKey} isCollapsed={isCollapsed} menu={footerMenu} />
                        </Footer>
                    </>
                )}
            </Content>
        </Container>
    );
};
