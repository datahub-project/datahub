import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { Link } from 'react-router-dom';
import { Tooltip } from 'antd';

import { useAppConfig } from '../../useAppConfig';
import { HOME_PAGE_INGESTION_ID } from '../../onboarding/config/HomePageOnboardingConfig';
import { useUserContext } from '../../context/useUserContext';
import { PageRoutes } from '../../../conf/Global';
import { useUpdateEducationStepsAllowList } from '../../onboarding/useUpdateEducationStepsAllowList';

import InboxMenuIcon from '../../../images/inboxMenuIcon.svg?react';
import AnalyticsMenuIcon from '../../../images/analyticsMenuIcon.svg?react';
import GovernMenuIcon from '../../../images/governMenuIcon.svg?react';
import ObserveMenuIcon from '../../../images/observeMenuIcon.svg?react';
import IngestionMenuIcon from '../../../images/ingestionMenuIcon.svg?react';
import SettingsMenuIcon from '../../../images/settingsMenuIcon.svg?react';

const LinksWrapper = styled.div<{ areLinksHidden?: boolean }>`
    opacity: 1;
    transition: opacity 0.5s;

    ${(props) => props.areLinksHidden && `
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
    color: #F9FAFC;

    &:hover { 
        cursor: pointer;
        background-color: #4B39BC;
        box-shadow: 0px 0px 8px 4px rgba(0, 0, 0, 0.15);
    }

    & svg {
        width: 24px;
        height: 24px;
        fill: #F9FAFC;
    }
`;

// Use to position the submenu
const SubMenu = styled.div`
    position: absolute;
    top: 3px;
    left: 50px;
    width: 200px;
    padding-left: 10px;
`;

// Used to style the submenu
const SubMenuContent = styled.div`
    border-radius: 12px;
    background: rgba(92, 63, 209, 0.95);
    box-shadow: 0px 8px 8px 4px rgba(0, 0, 0, 0.25);
    padding: 8px;

    & a {
        display: block;
        border-radius: 12px;
        color: #fff;
        font: 700 12px/20px Mulish;
        padding: 8px 12px;
        white-space: break-spaces;

        & span {
            display: block;
            font: 600 10px/12px Mulish;
        }

        &:hover {
            background-color: #4B39BC;
        }
    }
`;

const SubMenuTitle = styled.div`
    border-radius: 12px;
    background: #2F2477;
    padding: 8px 12px;
    font: 700 12px/20px Mulish;
    margin-bottom: 4px;
`;

interface Props {
    areLinksHidden?: boolean;
}

export function NavLinksMenu(props: Props) {
    const { areLinksHidden } = props;
    const me = useUserContext();
    const { config } = useAppConfig();

    // Submenu states
    const [showGovernMenu, setShowGovernMenu] = useState(false);
    const [showObserveMenu, setShowObserveMenu] = useState(false);

    // Flags to show/hide menu items
    const isAnalyticsEnabled = config?.analyticsConfig.enabled;
    const isIngestionEnabled = config?.managedIngestionConfig.enabled;
    const isActionRequestsEnabled = config?.actionRequestsConfig.enabled;
    const isTestsEnabled = config?.testsConfig.enabled;

    const showSettings = true;
    const showAnalytics = (isAnalyticsEnabled && me && me?.platformPrivileges?.viewAnalytics) || false;
    const showIngestion =
        isIngestionEnabled && me && me.platformPrivileges?.manageIngestion && me.platformPrivileges?.manageSecrets;
    const showActionRequests = (isActionRequestsEnabled && me?.platformPrivileges?.viewMetadataProposals) || false;
    const showTests = (isTestsEnabled && me?.platformPrivileges?.manageTests) || false;
    const showDatasetHealth = config?.featureFlags?.datasetHealthDashboardEnabled;
    const showObserve = showDatasetHealth;
    const showDocumentationCenter = config?.featureFlags?.documentationFormsEnabled || false; // TODO: Add platformPrivileges check

    // Update education steps allow list
    useUpdateEducationStepsAllowList(!!showIngestion, HOME_PAGE_INGESTION_ID);

    // Menu Items 
    const menuItems = [
        {
            icon: InboxMenuIcon,
            title: 'Inbox',
            description: 'Review and approve metadata proposals',
            link: PageRoutes.ACTION_REQUESTS,
            isHidden: !showActionRequests
        },
        {
            icon: AnalyticsMenuIcon,
            title: 'Analytics',
            description: 'Explore data usage and trends',
            link: PageRoutes.ANALYTICS,
            isHidden: !showAnalytics
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
                        title: 'Tests',
                        description: 'Monitor policies & automate actions across data assets',
                        link: PageRoutes.TESTS,
                        isHidden: !showTests,
                    },
                    {
                        title: 'Documentation',
                        description: 'Manage your documentation standards',
                        link: PageRoutes.GOVERN_DASHBOARD,
                        isHidden: !showDocumentationCenter,
                    }
                ]
            }
        },
        {
            icon: ObserveMenuIcon,
            title: 'Observe',
            description: 'Monitor data health and usage',
            link: null,
            isHidden: !showObserve,
            subMenu: {
                isOpen: showObserveMenu,
                open: () => setShowObserveMenu(true),
                close: () => setShowObserveMenu(false),
                items: [
                    {
                        title: 'Dataset Health',
                        description: 'Monitor active incidents & failing assertions across your organization\'s datasets',
                        link: PageRoutes.DATASET_HEALTH_DASHBOARD,
                        isHidden: !showDatasetHealth,
                    }
                ]
            }
        },
        {
            icon: IngestionMenuIcon,
            title: 'Ingestion',
            description: 'Manage data integrations and pipelines',
            link: PageRoutes.INGESTION,
            isHidden: !showIngestion
        },
        {
            icon: SettingsMenuIcon,
            title: 'Settings',
            description: 'Manage your account and preferences',
            link: PageRoutes.SETTINGS,
            isHidden: !showSettings
        },
        {
            icon: null,
            title: 'Help',
            description: 'Get help and support',
            link: null,
            isHidden: true
        }
    ];

    return (
        <LinksWrapper areLinksHidden={areLinksHidden}>
            {menuItems.map((menuItem) => {
                // If menu is hidden, don't show it
                if (menuItem.isHidden) return null;

                // Menu item has sub menu items
                const hasSubMenu = menuItem.subMenu?.items && menuItem.subMenu?.items.length > 0;

                // Return a menu item with a submenu
                if (hasSubMenu) {
                    const subMenu = (
                        <SubMenu>
                            <SubMenuContent>
                                <SubMenuTitle>{menuItem.title}</SubMenuTitle>
                                {menuItem.subMenu?.items.map((subMenuItem) => {
                                    if (subMenuItem.isHidden) return null;
                                    return (
                                        <Link
                                            key={subMenuItem.title.toLowerCase()}
                                            to={subMenuItem.link}
                                            aria-label={subMenuItem.title}
                                            aria-description={subMenuItem.description}
                                        >
                                            {subMenuItem.title}
                                            <span>{subMenuItem.description}</span>
                                        </Link>
                                    );
                                })}
                            </SubMenuContent>
                        </SubMenu>
                    );

                    return (
                        <LinkWrapper
                            aria-label={menuItem.title}
                            aria-description={menuItem.description}
                            onMouseEnter={menuItem.subMenu?.open}
                            onMouseLeave={menuItem.subMenu?.close}
                        >
                            {menuItem.icon && <menuItem.icon />}
                            {menuItem.subMenu?.isOpen && subMenu}
                        </LinkWrapper>
                    );
                }

                // Render a single menu item
                return (
                    <LinkWrapper key={menuItem.title.toLowerCase()}>
                        <Link
                            to={menuItem.link}
                            aria-label={menuItem.title}
                            aria-description={menuItem.description}
                        >
                            <Tooltip placement="right" title={menuItem.title}>
                                {menuItem.icon && <menuItem.icon />}
                            </Tooltip>
                        </Link>
                    </LinkWrapper>
                );
            })}
        </LinksWrapper>
    );
}
