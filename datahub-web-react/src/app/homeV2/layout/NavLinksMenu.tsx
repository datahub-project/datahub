import { Tooltip } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import styled, { useTheme } from 'styled-components/macro';

import { useUserContext } from '@app/context/useUserContext';
import CustomNavLink from '@app/homeV2/layout/CustomNavLink';
import { NavMenuItem, NavSubMenuItem } from '@app/homeV2/layout/types';
import { HOME_PAGE_INGESTION_ID } from '@app/onboarding/config/HomePageOnboardingConfig';
import { useHandleOnboardingTour } from '@app/onboarding/useHandleOnboardingTour';
import { useUpdateEducationStepsAllowList } from '@app/onboarding/useUpdateEducationStepsAllowList';
import { useAppConfig, useBusinessAttributesFlag } from '@app/useAppConfig';
import { HelpLinkRoutes, PageRoutes } from '@conf/Global';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

import AnalyticsMenuIcon from '@images/analyticsMenuIcon.svg?react';
import GovernMenuIcon from '@images/governMenuIcon.svg?react';
import HelpMenuIcon from '@images/help-icon.svg?react';
import IngestionMenuIcon from '@images/ingestionMenuIcon.svg?react';
import SettingsMenuIcon from '@images/settingsMenuIcon.svg?react';

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
    box-shadow: none;
    transition: all 200ms ease;
    color: ${(props) => props.theme.colors.textOnFillDefault};

    &:hover {
        cursor: pointer;
        background-color: ${(props) => props.theme.colors.bgSurfaceBrandHover};
        box-shadow: ${(props) => props.theme.colors.shadowMd};
    }

    & svg {
        width: 24px;
        height: 24px;
        fill: ${(props) => props.theme.colors.textOnFillDefault};
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
    background: ${(props) => props.theme.colors.bgSurfaceBrand};
    box-shadow: ${(props) => props.theme.colors.shadowLg};
    padding: 8px;
`;

const SubMenuTitle = styled.div`
    border-radius: 12px;
    background: ${(props) => props.theme.colors.bgSurfaceBrandHover};
    padding: 8px 12px;
    font: 700 12px/20px Mulish;
    color: ${(props) => props.theme.colors.textOnFillDefault};
    margin-bottom: 4px;
`;

const SubMenuLink = styled.div`
    border-radius: 12px;
    padding: 4px 12px 12px 12px;

    &:hover {
        background-color: ${(props) => props.theme.colors.bgSurfaceBrandHover};
    }
`;

interface Props {
    areLinksHidden?: boolean;
}

export function NavLinksMenu(props: Props) {
    const { t } = useTranslation('home.v2');
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
    const businessAttributesFlag = useBusinessAttributesFlag();

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
            title: t('navLinks.analytics.title'),
            description: t('navLinks.analytics.description'),
            link: PageRoutes.ANALYTICS,
            isHidden: !showAnalytics,
        },
        {
            icon: GovernMenuIcon,
            title: t('navLinks.govern.title'),
            description: t('navLinks.govern.description'),
            link: null,
            subMenu: {
                isOpen: showGovernMenu,
                open: () => setShowGovernMenu(true),
                close: () => setShowGovernMenu(false),
                items: [
                    {
                        title: t('navLinks.govern.glossary'),
                        description: t('navLinks.govern.glossaryDescription'),
                        link: PageRoutes.GLOSSARY,
                        isHidden: false,
                    },
                    {
                        title: t('navLinks.govern.tags'),
                        description: t('navLinks.govern.tagsDescription'),
                        link: PageRoutes.MANAGE_TAGS,
                        isHidden: false,
                    },
                    {
                        title: t('navLinks.govern.businessAttributes'),
                        description: t('navLinks.govern.businessAttributesDescription'),
                        link: PageRoutes.BUSINESS_ATTRIBUTE,
                        isHidden: !businessAttributesFlag,
                    },
                    {
                        title: t('navLinks.govern.domains'),
                        description: t('navLinks.govern.domainsDescription'),
                        link: PageRoutes.DOMAINS,
                        isHidden: false,
                    },
                    {
                        title: t('navLinks.govern.structuredProperties'),
                        showNewTag: true,
                        description: t('navLinks.govern.structuredPropertiesDescription'),
                        link: PageRoutes.STRUCTURED_PROPERTIES,
                        isHidden: !showStructuredProperties,
                    },
                ],
            },
        },
        {
            icon: IngestionMenuIcon,
            title: t('navLinks.ingestion.title'),
            description: t('navLinks.ingestion.description'),
            link: PageRoutes.INGESTION,
            isHidden: !showIngestion,
        },
        {
            icon: SettingsMenuIcon,
            title: t('navLinks.settings.title'),
            description: t('navLinks.settings.description'),
            link: PageRoutes.SETTINGS,
            isHidden: !showSettings,
        },
        {
            icon: HelpMenuIcon,
            title: t('navLinks.help.title'),
            description: t('navLinks.help.description'),
            link: null,
            isHidden: false,
            subMenu: {
                isOpen: showHelpMenu,
                open: () => setShowHelpMenu(true),
                close: () => setShowHelpMenu(false),
                items: [
                    {
                        title: t('navLinks.help.productTour'),
                        description: t('navLinks.help.productTourDescription'),
                        isHidden: false,
                        rel: 'noopener noreferrer',
                        onClick: showOnboardingTour,
                    },
                    {
                        title: t('navLinks.help.graphiql'),
                        description: t('navLinks.help.graphiqlDescription'),
                        link: HelpLinkRoutes.GRAPHIQL ? resolveRuntimePath(HelpLinkRoutes.GRAPHIQL) : null,
                        isHidden: false,
                        target: '_blank',
                        rel: 'noopener noreferrer',
                    },
                    {
                        title: t('navLinks.help.openapi'),
                        description: t('navLinks.help.openapiDescription'),
                        link: resolveRuntimePath(HelpLinkRoutes.OPENAPI),
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
