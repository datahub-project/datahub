import React, { useState } from 'react';
import styled, { useTheme } from 'styled-components/macro';

import { Tooltip } from '@components';
import { Link } from 'react-router-dom';

import { HelpLinkRoutes, PageRoutes } from '../../../conf/Global';
import { useUserContext } from '../../context/useUserContext';
import { HOME_PAGE_INGESTION_ID } from '../../onboarding/config/HomePageOnboardingConfig';
import { useUpdateEducationStepsAllowList } from '../../onboarding/useUpdateEducationStepsAllowList';
import { useAppConfig } from '../../useAppConfig';

import AnalyticsMenuIcon from '../../../images/analyticsMenuIcon.svg?react';
import GovernMenuIcon from '../../../images/governMenuIcon.svg?react';
import HelpMenuIcon from '../../../images/help-icon.svg?react';
import IngestionMenuIcon from '../../../images/ingestionMenuIcon.svg?react';
import SettingsMenuIcon from '../../../images/settingsMenuIcon.svg?react';
import { useHandleOnboardingTour } from '../../onboarding/useHandleOnboardingTour';
import CustomNavLink from './CustomNavLink';
import { NavMenuItem, NavSubMenuItem } from './types';

const LinksWrapper = styled.div<{ areLinksHidden?: boolean }>`
    opacity: 1;
    transition: opacity 0.5s;

    ${(props) =>
        props.areLinksHidden &&
        `
        opacity: 0;
        width: 0;
    `}
`;

const LinkWrapper = styled.span`
    position: relative;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 47px;
    height: 52px;
    width: 52px;
    line-height: 0;
    box-shadow: 0px 0px 8px 4px rgba(0, 0, 0, 0);
    transition: all 200ms ease;
    color: #f9fafc;

    &:hover {
        cursor: pointer;
        background-color: #4b39bc;
        box-shadow: 0px 0px 8px 4px rgba(0, 0, 0, 0.15);
    }

    & svg {
        width: 24px;
        height: 24px;
        fill: #f9fafc;
    }
`;

// Use to position the submenu
const SubMenu = styled.div`
    position: absolute;
    top: 3px;
    left: 50px;
    width: 220px;
    padding-left: 10px;
`;

// Used to style the submenu
const SubMenuContent = styled.div`
    border-radius: 12px;
    background: rgba(92, 63, 209, 0.95);
    box-shadow: 0px 8px 8px 4px rgba(0, 0, 0, 0.25);
    padding: 8px;
`;

const SubMenuTitle = styled.div`
    border-radius: 12px;
    background: #2f2477;
    padding: 8px 12px;
    font: 700 12px/20px Mulish;
    margin-bottom: 4px;
`;

const SubMenuLink = styled.div`
    border-radius: 12px;
    padding: 4px 12px 12px 12px;

    &:hover {
        background-color: #4b39bc;
    }
`;

interface Props {
    areLinksHidden?: boolean;
}

export function NavLinksMenu(props: Props) {
    const { areLinksHidden } = props;
    const me = useUserContext();
    const { config } = useAppConfig();
    const themeConfig = useTheme();
    const version = config?.appVersion;

    // Submenu states
    const [showGovernMenu, setShowGovernMenu] = useState(false);
    const [showHelpMenu, setShowHelpMenu] = useState(false);

    // Flags to show/hide menu items
    const isAnalyticsEnabled = config?.analyticsConfig?.enabled;
    const isIngestionEnabled = config?.managedIngestionConfig?.enabled;

    const showSettings = true;
    const showAnalytics = (isAnalyticsEnabled && me && me?.platformPrivileges?.viewAnalytics) || false;
    const showIngestion =
        isIngestionEnabled && me && me.platformPrivileges?.manageIngestion && me.platformPrivileges?.manageSecrets;

    const showStructuredProperties =
        config?.featureFlags?.showManageStructuredProperties &&
        (me.platformPrivileges?.manageStructuredProperties || me.platformPrivileges?.viewStructuredPropertiesPage);

    // Update education steps allow list
    useUpdateEducationStepsAllowList(!!showIngestion, HOME_PAGE_INGESTION_ID);

    const { showOnboardingTour } = useHandleOnboardingTour();

    // Help menu options
    const HelpContentMenuItems = themeConfig.content.menu.items.map((value) => ({
        title: value.label,
        description: value.description || '',
        link: value.path || null,
        isHidden: false,
        target: '_blank',
        rel: 'noopener noreferrer',
    })) as NavSubMenuItem[];

    // Menu Items
    const menuItems: Array<NavMenuItem> = [
        {
            icon: AnalyticsMenuIcon,
            title: 'Analytics',
            description: 'Explore data usage and trends',
            link: PageRoutes.ANALYTICS,
            isHidden: !showAnalytics,
        },
        {
            icon: GovernMenuIcon,
            title: 'Govern',
            description: 'Manage data access and quality',
            link: null,
            subMenu: {
                isOpen: showGovernMenu,
                open: () => setShowGovernMenu(true),
                close: () => setShowGovernMenu(false),
                items: [
                    {
                        title: 'Glossary',
                        description: 'View and modify your business glossary',
                        link: PageRoutes.GLOSSARY,
                        isHidden: false,
                    },
                    {
                        title: 'Domains',
                        description: 'Manage related groups of data assets',
                        link: PageRoutes.DOMAINS,
                        isHidden: false,
                    },
                    {
                        title: 'Structured Properties',
                        showNewTag: true,
                        description: `Manage custom properties for your data assets`,
                        link: PageRoutes.STRUCTURED_PROPERTIES,
                        isHidden: !showStructuredProperties,
                    },
                ],
            },
        },
        {
            icon: IngestionMenuIcon,
            title: 'Ingestion',
            description: 'Manage data integrations and pipelines',
            link: PageRoutes.INGESTION,
            isHidden: !showIngestion,
        },
        {
            icon: SettingsMenuIcon,
            title: 'Settings',
            description: 'Manage your account and preferences',
            link: PageRoutes.SETTINGS,
            isHidden: !showSettings,
        },
        {
            icon: HelpMenuIcon,
            title: 'Help',
            description: 'Explore help resources and documentation',
            link: null,
            isHidden: false,
            subMenu: {
                isOpen: showHelpMenu,
                open: () => setShowHelpMenu(true),
                close: () => setShowHelpMenu(false),
                items: [
                    {
                        title: 'Product Tour',
                        description: 'Take a quick tour of this page',
                        isHidden: false,
                        rel: 'noopener noreferrer',
                        onClick: showOnboardingTour,
                    },
                    {
                        title: 'GraphiQL',
                        description: 'Explore the GraphQL API',
                        link: HelpLinkRoutes.GRAPHIQL || null,
                        isHidden: false,
                        target: '_blank',
                        rel: 'noopener noreferrer',
                    },
                    {
                        title: 'OpenAPI',
                        description: 'Explore the OpenAPI endpoints',
                        link: HelpLinkRoutes.OPENAPI,
                        isHidden: false,
                        target: '_blank',
                        rel: 'noopener noreferrer',
                    },
                    ...HelpContentMenuItems,
                    {
                        title: version || '',
                        description: '',
                        link: null,
                        isHidden: !version,
                    },
                ],
            },
        },
    ];

    return (
        <LinksWrapper areLinksHidden={areLinksHidden} data-testid="nav-menu-links">
            {menuItems.map((menuItem) => {
                // If menu is hidden, don't show it
                if (menuItem.isHidden) return null;

                // Menu item has sub menu items
                const hasSubMenu = menuItem.subMenu?.items && menuItem.subMenu?.items?.length > 0;

                // Return a menu item with a submenu
                if (hasSubMenu) {
                    const subMenu = (
                        <SubMenu>
                            <SubMenuContent>
                                <SubMenuTitle>{menuItem.title}</SubMenuTitle>
                                {menuItem.subMenu?.items?.map((subMenuItem) => {
                                    if (subMenuItem.isHidden) return null;
                                    return (
                                        <SubMenuLink>
                                            <CustomNavLink
                                                menuItem={subMenuItem}
                                                key={subMenuItem.title.toLowerCase()}
                                            />
                                        </SubMenuLink>
                                    );
                                })}
                            </SubMenuContent>
                        </SubMenu>
                    );

                    return (
                        <LinkWrapper
                            aria-label={menuItem.title}
                            aria-describedby={menuItem.description}
                            onMouseEnter={menuItem.subMenu?.open}
                            onMouseLeave={menuItem.subMenu?.close}
                            key={menuItem.title.toLowerCase()}
                        >
                            {menuItem.icon && <menuItem.icon />}
                            {menuItem.subMenu?.isOpen && subMenu}
                        </LinkWrapper>
                    );
                }

                // Render a single menu item
                return (
                    <LinkWrapper key={menuItem.title.toLowerCase()}>
                        <Link to={menuItem.link} aria-label={menuItem.title} aria-describedby={menuItem.description}>
                            <Tooltip placement="right" title={menuItem.title} showArrow={false}>
                                {menuItem.icon && <menuItem.icon />}
                            </Tooltip>
                        </Link>
                    </LinkWrapper>
                );
            })}
        </LinksWrapper>
    );
}
