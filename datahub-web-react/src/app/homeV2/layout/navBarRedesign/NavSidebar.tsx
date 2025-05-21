import {
    BookBookmark,
    FileLock,
    Gear,
    Globe,
    Heartbeat,
    Lightning,
    ListChecks,
    Plugs,
    Question,
    SignOut,
    SquaresFour,
    Tag,
    TestTube,
    TextColumns,
    TrendUp,
    UserCircle,
} from '@phosphor-icons/react';
import React, { useContext, useEffect } from 'react';
import styled, { useTheme } from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { useNavBarContext } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
import NavBarHeader from '@app/homeV2/layout/navBarRedesign/NavBarHeader';
import NavBarMenu from '@app/homeV2/layout/navBarRedesign/NavBarMenu';
import NavSkeleton from '@app/homeV2/layout/navBarRedesign/NavBarSkeleton';
import {
    NavBarMenuDropdownItemElement,
    NavBarMenuItemTypes,
    NavBarMenuItems,
} from '@app/homeV2/layout/navBarRedesign/types';
import useSelectedKey from '@app/homeV2/layout/navBarRedesign/useSelectedKey';
import OnboardingContext from '@app/onboarding/OnboardingContext';
import { useAppConfig } from '@app/useAppConfig';
import { colors } from '@src/alchemy-components';
import { getColor } from '@src/alchemy-components/theme/utils';
import useGetLogoutHandler from '@src/app/auth/useGetLogoutHandler';
import { useGlobalSettingsContext } from '@src/app/context/GlobalSettings/GlobalSettingsContext';
import { HOME_PAGE_INGESTION_ID } from '@src/app/onboarding/config/HomePageOnboardingConfig';
import { useHandleOnboardingTour } from '@src/app/onboarding/useHandleOnboardingTour';
import { useUpdateEducationStepsAllowList } from '@src/app/onboarding/useUpdateEducationStepsAllowList';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { HelpLinkRoutes, PageRoutes } from '@src/conf/Global';
import { generateReleaseNotesLink } from '@src/conf/utils';
import { EntityType } from '@src/types.generated';

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
    padding: 17px 8px 17px 16px;
    height: 100%;
    width: ${(props) => (props.isCollapsed ? '60px' : '264px')};
    transition: width 250ms ease-in-out;
    overflow-x: hidden;
`;

const CustomLogo = styled.img`
    object-fit: contain;
    max-height: 26px;
    max-width: 26px;
    min-height: 20px;
    min-width: 20px;
`;

const Spacer = styled.div`
    flex: 1;
`;

const DEFAULT_LOGO = '/assets/logos/acryl-dark-mark.svg';

const MenuWrapper = styled.div`
    margin-top: 14px;
    height: 100%;
`;

export const NavSidebar = () => {
    const entityRegistry = useEntityRegistry();
    const themeConfig = useTheme();

    const { isCollapsed, selectedKey, setSelectedKey } = useNavBarContext();
    const appConfig = useAppConfig();
    const userContext = useUserContext();
    const me = useUserContext();

    const { isUserInitializing } = useContext(OnboardingContext);
    const { showOnboardingTour } = useHandleOnboardingTour();
    const { config } = useAppConfig();
    const logout = useGetLogoutHandler();

    const showAnalytics = (config?.analyticsConfig?.enabled && me && me?.platformPrivileges?.viewAnalytics) || false;
    const showStructuredProperties =
        config?.featureFlags?.showManageStructuredProperties &&
        (me.platformPrivileges?.manageStructuredProperties || me.platformPrivileges?.viewStructuredPropertiesPage);
    const showManageTags =
        config?.featureFlags?.showManageTags &&
        (me.platformPrivileges?.manageTags || me.platformPrivileges?.viewManageTags);

    /* SaaS Only */
    const { helpLinkState, globalSettings } = useGlobalSettingsContext();
    const { showFormAnalytics, formCreationEnabled } = config.featureFlags;
    const showActionRequests = config?.actionRequestsConfig?.enabled || false;
    const showTests = ((config?.testsConfig?.enabled || false) && me?.platformPrivileges?.manageTests) || false;
    const showAddHelpLink = !helpLinkState.isEnabled && me.platformPrivileges?.manageGlobalSettings;
    const showAutomations = config?.classificationConfig?.enabled && me?.platformPrivileges?.manageIngestion;
    const showDocumentationCenter =
        config?.featureFlags?.documentationFormsEnabled &&
        (me.platformPrivileges?.manageDocumentationForms || me.platformPrivileges?.viewDocumentationFormsPage) &&
        (showFormAnalytics || formCreationEnabled);
    const showDatasetHealth =
        config?.featureFlags?.datasetHealthDashboardEnabled || config?.featureFlags?.datasetHealthDashboardV2Enabled;
    /* End SaaS Only */

    const showDataSources =
        config.managedIngestionConfig.enabled &&
        (me.platformPrivileges?.manageIngestion || me.platformPrivileges?.manageSecrets);

    // Update education steps allow list
    useUpdateEducationStepsAllowList(!!showDataSources, HOME_PAGE_INGESTION_ID);

    const customLogoUrl = globalSettings?.visualSettings?.customLogoUrl || appConfig.config.visualConfig.logoUrl;
    const hasCustomLogo = customLogoUrl && customLogoUrl !== DEFAULT_LOGO;
    const logoComponent = hasCustomLogo ? <CustomLogo alt="logo" src={customLogoUrl} /> : <AcrylIcon />;

    const {
        state: { unfinishedTaskCount },
    } = userContext;

    const HelpContentMenuItems = themeConfig.content.menu.items.map((value) => ({
        title: value.label,
        description: value.description || '',
        link: value.path,
        isHidden: false,
        isExternalLink: true,
        key: `helpMenu${value.label}`,
    })) as NavBarMenuDropdownItemElement[];

    const versionLink = generateReleaseNotesLink(config?.appVersion);

    const mainMenu: NavBarMenuItems = {
        items: [
            {
                type: NavBarMenuItemTypes.Item,
                title: 'Home',
                icon: <SquaresFour />,
                selectedIcon: <SquaresFour weight="fill" />,
                key: 'home',
                link: PageRoutes.ROOT,
                onlyExactPathMapping: true,
            },
            {
                type: NavBarMenuItemTypes.Item,
                title: 'Tasks',
                icon: <ListChecks />,
                selectedIcon: <ListChecks weight="fill" />,
                key: 'tasks',
                isHidden: !showActionRequests,
                link: PageRoutes.ACTION_REQUESTS,
                badge: {
                    count: unfinishedTaskCount,
                    show: unfinishedTaskCount > 0,
                },
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
                        title: 'Domains',
                        key: 'domains',
                        icon: <Globe />,
                        selectedIcon: <Globe weight="fill" />,
                        link: PageRoutes.DOMAINS,
                        additionalLinksForPathMatching: [`/${entityRegistry.getPathName(EntityType.Domain)}/:urn`],
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Tests',
                        key: 'tests',
                        isHidden: !showTests,
                        icon: <TestTube />,
                        selectedIcon: <TestTube weight="fill" />,
                        link: PageRoutes.TESTS,
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Automations',
                        description: 'Manage automated actions across your data assets',
                        icon: <Lightning />,
                        selectedIcon: <Lightning weight="fill" />,
                        key: 'automations',
                        link: PageRoutes.AUTOMATIONS,
                        isHidden: !showAutomations,
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Compliance Forms',
                        key: 'complianceForms',
                        isHidden: !showDocumentationCenter,
                        icon: <FileLock />,
                        selectedIcon: <FileLock weight="fill" />,
                        link: PageRoutes.GOVERN_DASHBOARD,
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
                key: 'observe',
                title: 'Observe',
                items: [
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Data Health',
                        key: 'dataHealth',
                        isHidden: !showDatasetHealth,
                        icon: <Heartbeat />,
                        selectedIcon: <Heartbeat weight="fill" />,
                        link: PageRoutes.DATASET_HEALTH_DASHBOARD,
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
                        link: PageRoutes.INGESTION,
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
            {
                type: NavBarMenuItemTypes.Custom,
                key: 'spacer',
                render: () => <Spacer />,
            },
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
                        title: helpLinkState.label,
                        link: helpLinkState.link,
                        isExternalLink: true,
                        isHidden: !helpLinkState.isEnabled,
                        key: 'helpHelp',
                    },
                    {
                        type: NavBarMenuItemTypes.DropdownElement,
                        title: 'Product Tour',
                        description: 'Take a quick tour of this page',
                        key: 'helpProductTour',
                        onClick: showOnboardingTour,
                    },
                    {
                        type: NavBarMenuItemTypes.DropdownElement,
                        title: 'GraphQL',
                        description: 'Explore the GraphQL API',
                        link: HelpLinkRoutes.GRAPHIQL || null,
                        isExternalLink: true,
                        key: 'helpGraphQL',
                    },
                    {
                        type: NavBarMenuItemTypes.DropdownElement,
                        title: 'OpenAPI',
                        description: 'Explore the OpenAPI endpoints',
                        link: HelpLinkRoutes.OPENAPI,
                        isExternalLink: true,
                        key: 'helpOpenAPI',
                    },
                    ...HelpContentMenuItems,
                    {
                        type: NavBarMenuItemTypes.DropdownElement,
                        title: 'Add Custom Help Link',
                        link: PageRoutes.SETTINGS_HELP_LINK,
                        key: 'helpAddCustomHelpLink',
                        isHidden: !showAddHelpLink,
                    },
                    {
                        type: NavBarMenuItemTypes.DropdownElement,
                        title: config?.appVersion || '',
                        isHidden: !config?.appVersion,
                        link: versionLink,
                        isExternalLink: true,
                        key: 'helpAppVersion',
                        disabled: !versionLink,
                    },
                ],
            },
            {
                type: NavBarMenuItemTypes.Item,
                title: 'Sign out',
                icon: <SignOut data-testid="log-out-menu-item" />,
                key: 'signOut',
                onClick: logout,
                href: '/logOut',
                dataTestId: 'nav-sidebar-sign-out',
            },
        ],
    };
    const sk = useSelectedKey(mainMenu);

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
            <Content isCollapsed={isCollapsed}>
                {showSkeleton ? (
                    <NavSkeleton isCollapsed={isCollapsed} />
                ) : (
                    <>
                        <NavBarHeader logotype={logoComponent} />
                        <MenuWrapper>
                            <NavBarMenu selectedKey={selectedKey} isCollapsed={isCollapsed} menu={mainMenu} />
                        </MenuWrapper>
                    </>
                )}
            </Content>
        </Container>
    );
};
