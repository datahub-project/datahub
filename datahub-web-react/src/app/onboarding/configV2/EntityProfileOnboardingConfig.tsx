import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

export const ENTITY_PROFILE_V2_COLUMNS_ID = 'entity-profile-v2-columns';
export const ENTITY_PROFILE_V2_CONTENTS_ID = 'entity-profile-v2-contents';
export const ENTITY_PROFILE_V2_SUBSCRIPTION_ID = 'entity-profile-v2-subscriptions';
export const ENTITY_PROFILE_V2_DOCUMENTATION_ID = 'entity-profile-v2-documentation';
export const ENTITY_PROFILE_V2_SIDEBAR_ID = 'entity-profile-v2-sidebar';
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
        title: 'Columns 🧮',
        content: (
            <Typography.Paragraph>
                <p>
                    You can view an asset&apos;s <strong>Fields</strong> on this tab.
                </p>
                <p>
                    You can also view or add <strong>Documentation</strong>, <strong>Tags</strong>, and{' '}
                    <strong>Glossary Terms</strong> for specific fields.
                </p>
                <p>Click on individual fields to view more details such as statistics and properties.</p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_PROFILE_V2_CONTENTS_ID,
        selector: `[id^='rc-tabs'][id$='Contents']`,
        title: 'Contents 📦',
        content: (
            <Typography.Paragraph>
                <p>
                    You can view the child <strong>Assets</strong> that belong to this <strong>Asset</strong> on this
                    tab.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_PROFILE_V2_DOCUMENTATION_ID,
        selector: `[id^='rc-tabs'][id$='Documentation']`,
        title: 'Documentation 📖',
        content: (
            <Typography.Paragraph>
                <p>
                    You can view and edit rich <strong>Documentation</strong> on this tab.
                </p>
                <p>
                    <strong>Documentation</strong> should provide descriptive information about this data asset to help
                    your data explorers understand it better. It can also contain links to external resources.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_PROFILE_V2_SIDEBAR_ID,
        selector: `#${ENTITY_PROFILE_V2_SIDEBAR_ID}`,
        title: 'Introducing the Asset Sidebar',
        content: (
            <Typography.Paragraph>
                <p>
                    The asset sidebar is a vertically organized set of important information about an asset. It appears
                    on the right side of the screen when viewing an asset, a search result, a lineage entry, and many
                    other places.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_SIDEBAR_V2_ABOUT_TAB_ID,
        selector: `[id^='entity-sidebar-tabs'][id$='About']`,
        title: 'Summary',
        content: (
            <Typography.Paragraph>
                <p>Quick access to at-a-glance information about the asset.</p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_SIDEBAR_V2_COLUMNS_TAB_ID,
        selector: `[id^='entity-sidebar-tabs'][id$='Columns']`,
        title: 'Columns',
        content: (
            <Typography.Paragraph>
                <p>
                    Quick access to an asset&apos;s <strong>Fields</strong> on this tab.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_SIDEBAR_V2_LINEAGE_TAB_ID,
        selector: `[id^='entity-sidebar-tabs'][id$='Lineage']`,
        title: 'Lineage 🕸️',
        content: (
            <Typography.Paragraph>
                <p>Quick access to upstream and downstream lineage, both direct and indirect.</p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_SIDEBAR_V2_PROPERTIES_ID,
        selector: `[id^='entity-sidebar-tabs'][id$='Properties']`,
        title: 'Properties 📑',
        content: (
            <Typography.Paragraph>
                <p>
                    Properties have moved to the asset sidebar! You can view and (soon) edit <strong>Properties</strong>{' '}
                    on this tab.
                </p>
                <p>
                    Properties are key value pairs that provide additional information about an asset. Some properties
                    are ingested from the source data platform, while others are added by users.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_PROFILE_V2_QUERIES_ID,
        selector: `[id^='rc-tabs'][id$='Queries']`,
        title: 'Queries 🖥️',
        content: (
            <Typography.Paragraph>
                <p>
                    View highlighted and relevant <strong>Queries</strong> on this tab.
                </p>
                <p>Highlighted queries are handpicked by your data experts to help you get started with this asset.</p>
                <p>Relevant queries are computed based on recency, popularity, and other factors.</p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_PROFILE_V2_VALIDATION_ID,
        selector: `[id^='rc-tabs'][id$='Quality']`,
        title: 'Quality ✔️ ',
        content: (
            <Typography.Paragraph>
                <p>
                    View <strong>Quality</strong> information on this tab.
                </p>
                <p>
                    Quality information includes <strong>data contracts</strong>and data quality test results.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_PROFILE_V2_INCIDENTS_ID,
        selector: `[id^='rc-tabs'][id$='Incidents']`,
        title: 'Incidents ⚠️',
        content: (
            <Typography.Paragraph>
                <p>
                    View and manage <strong>Incidents</strong> on this tab.
                </p>
                <p>
                    Incidents are issues or events that require attention. They can be related to data quality,
                    governance, schema changes, and more.
                </p>
            </Typography.Paragraph>
        ),
    },
];

export default EntityProfileOnboardingConfig;
