import React, { useContext } from 'react';
import {
    ApiOutlined,
    BookOutlined,
    DatabaseOutlined,
    FileDoneOutlined,
    FormOutlined,
    QuestionCircleOutlined,
    SettingOutlined,
    UnorderedListOutlined,
} from '@ant-design/icons';
import { useGlobalSettingsContext } from '@src/app/context/GlobalSettings/GlobalSettingsContext';
import DomainIcon from '@src/app/domainV2/DomainIcon';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { HOME_PAGE_INGESTION_ID } from '@src/app/onboarding/config/HomePageOnboardingConfig';
import { useHandleOnboardingTour } from '@src/app/onboarding/useHandleOnboardingTour';
import { useUpdateEducationStepsAllowList } from '@src/app/onboarding/useUpdateEducationStepsAllowList';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { HelpLinkRoutes, PageRoutes } from '@src/conf/Global';
import { EntityType } from '@src/types.generated';
import { FaRegUserCircle } from 'react-icons/fa';
import { HiOutlineClipboardList } from 'react-icons/hi';
import { Lightning, SquaresFour, TrendUp } from '@phosphor-icons/react';
import styled, { useTheme } from 'styled-components';
import AcrylIcon from '../../../../images/acryl-light-mark.svg?react';
import { useUserContext } from '../../../context/useUserContext';
import OnboardingContext from '../../../onboarding/OnboardingContext';
import { useAppConfig } from '../../../useAppConfig';
import NavBarHeader from './NavBarHeader';
import NavBarMenu from './NavBarMenu';
import NavSkeleton from './NavBarSkeleton';
import { NavBarMenuDropdownItemElement, NavBarMenuItems, NavBarMenuItemTypes } from './types';

const Container = styled.div`
    height: 100vh;
    background-color: ${REDESIGN_COLORS.BACKGROUUND_NAVBAR_REDESIGN};
`;

const Content = styled.div`
    display: flex;
    flex-direction: column;
    padding: 16px;
    height: 100%;
    width: 267px;
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

    const showIngestion =
        config.managedIngestionConfig.enabled &&
        me &&
        me.platformPrivileges?.manageIngestion &&
        me.platformPrivileges?.manageSecrets;

    // Update education steps allow list
    useUpdateEducationStepsAllowList(!!showIngestion, HOME_PAGE_INGESTION_ID);

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
                key: 'home',
                link: PageRoutes.ROOT,
            },
            {
                type: NavBarMenuItemTypes.Item,
                title: 'Tasks',
                icon: <HiOutlineClipboardList />,
                key: 'tasks',
                isHidden: !showActionRequests,
                link: PageRoutes.ACTION_REQUESTS,
            },
            {
                type: NavBarMenuItemTypes.Item,
                title: 'Analytics',
                icon: <TrendUp />,
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
                        icon: <BookOutlined />,
                        link: PageRoutes.GLOSSARY,
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Domains',
                        key: 'domains',
                        icon: <DomainIcon />,
                        link: PageRoutes.DOMAINS,
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Automations',
                        description: 'Manage automated actions across your data assets',
                        icon: <Lightning />,
                        key: 'automations',
                        link: PageRoutes.AUTOMATIONS,
                        isHidden: !showAutomations,
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Tests',
                        key: 'tests',
                        isHidden: !showTests,
                        icon: <FileDoneOutlined />,
                        link: PageRoutes.TESTS,
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Compliance Forms',
                        key: 'complianceForms',
                        isHidden: !showDocumentationCenter,
                        icon: <FormOutlined />,
                        link: PageRoutes.GOVERN_DASHBOARD,
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Structured Properties',
                        key: 'structuredProperties',
                        isHidden: !showStructuredProperties,
                        icon: <UnorderedListOutlined />,
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
                        icon: <DatabaseOutlined />,
                        link: PageRoutes.DATASET_HEALTH_DASHBOARD,
                    },
                    {
                        type: NavBarMenuItemTypes.Item,
                        title: 'Ingestion',
                        key: 'ingestion',
                        isHidden: !showIngestion,
                        icon: <ApiOutlined />,
                        link: PageRoutes.INGESTION,
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
                icon: <FaRegUserCircle />,
                key: 'profile',
                link: `/${entityRegistry.getPathName(EntityType.CorpUser)}/${userContext.urn}`,
            },
            {
                type: NavBarMenuItemTypes.Item,
                title: 'Settings',
                icon: <SettingOutlined />,
                key: 'settings',
                link: '/settings',
            },
            {
                type: NavBarMenuItemTypes.Dropdown,
                title: 'Help',
                icon: <QuestionCircleOutlined />,
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
            <Content>
                {showSkeleton ? (
                    <NavSkeleton />
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
