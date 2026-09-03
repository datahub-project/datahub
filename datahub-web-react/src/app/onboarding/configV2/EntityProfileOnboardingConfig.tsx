import { Text } from '@components';
import React from 'react';
import { Trans } from 'react-i18next';

import { OnboardingStep } from '@app/onboarding/OnboardingStep';

export const ENTITY_PROFILE_V2_COLUMNS_ID = 'entity-profile-v2-columns';
export const ENTITY_PROFILE_V2_CONTENTS_ID = 'entity-profile-v2-contents';
export const ENTITY_PROFILE_V2_DOCUMENTATION_ID = 'entity-profile-v2-documentation';
const ENTITY_PROFILE_V2_SIDEBAR_ID = 'entity-profile-v2-sidebar';
export const ENTITY_SIDEBAR_V2_ABOUT_TAB_ID = 'entity-profile-v2-about';
export const ENTITY_SIDEBAR_V2_COLUMNS_TAB_ID = 'entity-sidebar-v2-columns-tab';
export const ENTITY_SIDEBAR_V2_LINEAGE_TAB_ID = 'entity-profile-v2-lineage-tab';
export const ENTITY_SIDEBAR_V2_PROPERTIES_ID = 'entity-sidebar-v2-properties';
export const ENTITY_PROFILE_V2_QUERIES_ID = 'entity-profile-v2-queries';
export const ENTITY_PROFILE_V2_VALIDATION_ID = 'entity-profile-v2-validation';
export const ENTITY_PROFILE_V2_INCIDENTS_ID = 'entity-profile-v2-incidents';

const EntityProfileOnboardingConfig: OnboardingStep[] = [
    {
        id: ENTITY_PROFILE_V2_COLUMNS_ID,
        selector: `[id^='rc-tabs'][id$='Columns']`,
        title: <Trans i18nKey="onboarding:entityProfileV2.columnsTitle" />,
        tabName: 'Columns',
        action: (node) => {
            // Scroll the tab into view and ensure it's selected
            if (node) {
                node.scrollIntoView({ behavior: 'auto', block: 'start', inline: 'nearest' });
            }
        },
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:entityProfileV2.columnsDescription1" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:entityProfileV2.columnsDescription2" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:entityProfileV2.columnsDescription3" />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_PROFILE_V2_CONTENTS_ID,
        selector: `[id^='rc-tabs'][id$='Contents']`,
        title: <Trans i18nKey="onboarding:entityProfileV2.contentsTitle" />,
        tabName: 'Contents',
        action: (node) => {
            if (node) {
                node.scrollIntoView({ behavior: 'auto', block: 'start', inline: 'nearest' });
            }
        },
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:entityProfileV2.contentsDescription" components={{ bold: <strong /> }} />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_PROFILE_V2_DOCUMENTATION_ID,
        selector: `[id^='rc-tabs'][id$='Documentation']`,
        title: <Trans i18nKey="onboarding:entityProfileV2.documentationTitle" />,
        tabName: 'Documentation',
        action: (node) => {
            if (node) {
                node.scrollIntoView({ behavior: 'auto', block: 'start', inline: 'nearest' });
            }
        },
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans
                        i18nKey="onboarding:entityProfileV2.documentationDescription1"
                        components={{ bold: <strong /> }}
                    />
                </p>
                <p>
                    <Trans
                        i18nKey="onboarding:entityProfileV2.documentationDescription2"
                        components={{ bold: <strong /> }}
                    />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_PROFILE_V2_SIDEBAR_ID,
        selector: `#${ENTITY_PROFILE_V2_SIDEBAR_ID}`,
        title: <Trans i18nKey="onboarding:entityProfile.sidebarTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:entityProfileV2.sidebarDescription" />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_SIDEBAR_V2_ABOUT_TAB_ID,
        selector: `[id^='entity-sidebar-tabs'][id$='About']`,
        title: <Trans i18nKey="onboarding:entityProfileV2.summaryTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:entityProfileV2.summaryDescription" />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_SIDEBAR_V2_COLUMNS_TAB_ID,
        selector: `[id^='entity-sidebar-tabs'][id$='Columns']`,
        title: <Trans i18nKey="common.labels:columns" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans
                        i18nKey="onboarding:entityProfileV2.columnsTabDescription"
                        components={{ bold: <strong /> }}
                    />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_SIDEBAR_V2_LINEAGE_TAB_ID,
        selector: `[id^='entity-sidebar-tabs'][id$='Lineage']`,
        title: <Trans i18nKey="onboarding:entityProfileV2.lineageTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:entityProfileV2.lineageDescription" />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_SIDEBAR_V2_PROPERTIES_ID,
        selector: `[id^='entity-sidebar-tabs'][id$='Properties']`,
        title: <Trans i18nKey="onboarding:entityProfileV2.propertiesTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans
                        i18nKey="onboarding:entityProfileV2.propertiesDescription1"
                        components={{ bold: <strong /> }}
                    />
                </p>
                <p>
                    <Trans i18nKey="onboarding:entityProfileV2.propertiesDescription2" />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_PROFILE_V2_QUERIES_ID,
        selector: `[id^='rc-tabs'][id$='Queries']`,
        title: <Trans i18nKey="onboarding:entityProfileV2.queriesTitle" />,
        tabName: 'Queries',
        action: (node) => {
            if (node) {
                node.scrollIntoView({ behavior: 'auto', block: 'start', inline: 'nearest' });
            }
        },
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:entityProfileV2.queriesDescription1" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:entityProfileV2.queriesDescription2" />
                </p>
                <p>
                    <Trans i18nKey="onboarding:entityProfileV2.queriesDescription3" />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_PROFILE_V2_VALIDATION_ID,
        selector: `[id^='rc-tabs'][id$='Quality']`,
        title: <Trans i18nKey="onboarding:entityProfileV2.qualityTitle" />,
        tabName: 'Quality',
        action: (node) => {
            if (node) {
                node.scrollIntoView({ behavior: 'auto', block: 'start', inline: 'nearest' });
            }
        },
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:entityProfileV2.qualityDescription1" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:entityProfileV2.qualityDescription2" components={{ bold: <strong /> }} />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_PROFILE_V2_INCIDENTS_ID,
        selector: `[id^='rc-tabs'][id$='Incidents']`,
        title: <Trans i18nKey="onboarding:entityProfileV2.incidentsTitle" />,
        tabName: 'Incidents',
        action: (node) => {
            if (node) {
                node.scrollIntoView({ behavior: 'auto', block: 'start', inline: 'nearest' });
            }
        },
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans
                        i18nKey="onboarding:entityProfileV2.incidentsDescription1"
                        components={{ bold: <strong /> }}
                    />
                </p>
                <p>
                    <Trans i18nKey="onboarding:entityProfileV2.incidentsDescription2" />
                </p>
            </Text>
        ),
    },
];

export default EntityProfileOnboardingConfig;
