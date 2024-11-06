import React, { useContext } from 'react';
import { useGlobalSettingsContext } from '@src/app/context/GlobalSettings/GlobalSettingsContext';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { HOME_PAGE_INGESTION_ID } from '@src/app/onboarding/config/HomePageOnboardingConfig';
import { useHandleOnboardingTour } from '@src/app/onboarding/useHandleOnboardingTour';
import { useUpdateEducationStepsAllowList } from '@src/app/onboarding/useUpdateEducationStepsAllowList';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { HelpLinkRoutes, PageRoutes } from '@src/conf/Global';
import { EntityType } from '@src/types.generated';
import {
    BuildingOffice,
    Cylinder,
    FileLock,
    Gear,
    Heartbeat,
    Lightning,
    ListChecks,
    Plugs,
    Question,
    SquaresFour,
    TestTube,
    TextColumns,
    TrendUp,
    UserCircle,
} from '@phosphor-icons/react';
import styled, { useTheme } from 'styled-components';
import AcrylIcon from '../../../../images/acryl-light-mark.svg?react';
import { useUserContext } from '../../../context/useUserContext';
import OnboardingContext from '../../../onboarding/OnboardingContext';
import { useAppConfig } from '../../../useAppConfig';
import NavBarHeader from './NavBarHeader';
import NavBarMenu from './NavBarMenu';
import NavSkeleton from './NavBarSkeleton';
import { NavBarMenuDropdownItemElement, NavBarMenuItems, NavBarMenuItemTypes } from './types';
import { useNavBarContext } from './NavBarContext';

const Container = styled.div`
    height: 100vh;
    background-color: ${REDESIGN_COLORS.BACKGROUUND_NAVBAR_REDESIGN};
    display: flex;
    flex: column;
    align-items: center;
`;

const Content = styled.div<{ isCollapsed: boolean }>`
    display: flex;
    flex-direction: column;
    padding: 17px 9px 17px 17px;
    height: 100%;
    width: ${(props) => (props.isCollapsed ? '60px' : '267px')};
    transition: width 250ms ease-in-out;
    overflow-x: hidden;
`;

const CustomLogo = styled.img`
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
    margin-top: 40px;
    height: 100%;
`;

export const NavSidebar = () => {
    const entityRegistry = useEntityRegistry();
    const themeConfig = useTheme();

    const { isCollapsed } = useNavBarContext();
    const appConfig = useAppConfig();
    const userContext = useUserContext();
    const me = useUserContext();

    const { isUserInitializing } = useContext(OnboardingContext);
    const { helpLinkState } = useGlobalSettingsContext();
    const { showOnboardingTour } = useHandleOnboardingTour();
    const { config } = useAppConfig();

    const showActionRequests = config?.actionRequestsConfig.enabled || false;
    const showTests = ((config?.testsConfig.enabled || false) && me?.platformPrivileges?.manageTests) || false;
    const showAddHelpLink = !helpLinkState.isEnabled && me.platformPrivileges?.manageGlobalSettings;
    const showAnalytics = (config?.analyticsConfig.enabled && me && me?.platformPrivileges?.viewAnalytics) || false;
    const showAutomations = config?.classificationConfig.enabled && me?.platformPrivileges?.manageIngestion;
    const showDocumentationCenter =
        config?.featureFlags?.documentationFormsEnabled &&
        (me.platformPrivileges?.manageDocumentationForms || me.platformPrivileges?.viewDocumentationFormsPage);
    const showStructuredProperties =
        config?.featureFlags?.showManageStructuredProperties &&
        (me.platformPrivileges?.manageStructuredProperties || me.platformPrivileges?.viewStructuredPropertiesPage);
    const showDatasetHealth = config?.featureFlags?.datasetHealthDashboardEnabled;

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

    const mainMenu: NavBarMenuItems = {
        items: [
            {
                type: NavBarMenuItemTypes.Item,
                title: 'Home',
                icon: <SquaresFour />,
                selectedIcon: <SquaresFour weight="fill" />,
                key: 'home',
                link: PageRoutes.ROOT,
            },
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
                title: 'Tasks',
                icon: <ListChecks />,
                selectedIcon: <ListChecks weight="fill" />,
                key: 'tasks',
                isHidden: !showActionRequests,
                link: PageRoutes.ACTION_REQUESTS,
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
            {
                type: NavBarMenuItemTypes.Group,
                key: 'govern',
                title: 'Govern',
                items: [
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Glossary',
                        key: 'glossary',
                        icon: <Cylinder />,
                        selectedIcon: <Cylinder weight="fill" />,
                        link: PageRoutes.GLOSSARY,
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Domains',
                        key: 'domains',
                        icon: <BuildingOffice />,
                        selectedIcon: <BuildingOffice weight="fill" />,
                        link: PageRoutes.DOMAINS,
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
                        title: config?.appVersion || '',
                        isHidden: !config?.appVersion,
                        key: 'helpAppVersion',
                    },
                    {
                        type: NavBarMenuItemTypes.DropdownElement,
                        title: 'Add Custom Help Link',
                        link: PageRoutes.SETTINGS_HELP_LINK,
                        key: 'helpAddCustomHelpLink',
                        isHidden: !showAddHelpLink,
                    },
                ],
            },
        ],
    };

    const showSkeleton = isUserInitializing || !appConfig.loaded || !userContext.loaded;

    return (
        <Container>
            <Content isCollapsed={isCollapsed}>
                {showSkeleton ? (
                    <NavSkeleton isCollapsed={isCollapsed} />
                ) : (
                    <>
                        <NavBarHeader logotype={logoComponent} />
                        <MenuWrapper>
                            <NavBarMenu menu={mainMenu} />
                        </MenuWrapper>
                    </>
                )}
            </Content>
        </Container>
    );
};
