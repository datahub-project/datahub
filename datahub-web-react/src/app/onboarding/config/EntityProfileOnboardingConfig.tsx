import React from 'react';
import { Typography } from 'antd';
import { OnboardingStep } from '../OnboardingStep';

// Entity profile tabs. Note that the 'rc-tab' prefix for the ID is added by the antd library and may change in the future.
export const ENTITY_PROFILE_ENTITIES_ID = 'entity-profile-entities';
export const ENTITY_PROFILE_PROPERTIES_ID = 'entity-profile-properties';
export const ENTITY_PROFILE_DOCUMENTATION_ID = 'entity-profile-documentation';
export const ENTITY_PROFILE_LINEAGE_ID = 'entity-profile-lineage';
export const ENTITY_PROFILE_SCHEMA_ID = 'entity-profile-schema';

// Entity profile sidebar
export const ENTITY_PROFILE_OWNERS_ID = 'entity-profile-owners';
export const ENTITY_PROFILE_TAGS_ID = 'entity-profile-tags';
export const ENTITY_PROFILE_GLOSSARY_TERMS_ID = 'entity-profile-glossary-terms';
export const ENTITY_PROFILE_DOMAINS_ID = 'entity-profile-domains';

export const EntityProfileOnboardingConfig: OnboardingStep[] = [
    {
        id: ENTITY_PROFILE_ENTITIES_ID,
        selector: `[id^='rc-tabs'][id$='Entities']`,
        title: 'Entities Tab',
        content: (
            <Typography.Paragraph>
                <p>
                    You can view the <strong>Entities</strong> that belong to a <strong>Container</strong> on this tab.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_PROFILE_PROPERTIES_ID,
        selector: `[id^='rc-tabs'][id$='Properties']`,
        title: 'Properties Tab',
        content: (
            <Typography.Paragraph>
                <p>
                    You can view an entity&apos;s key-value <strong>Properties</strong> on this tab. These are sourced
                    from the original Data Platform.
                </p>
                <p>
                    If this tab is disabled, <strong>Properties</strong> have not been ingested for this entity.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_PROFILE_DOCUMENTATION_ID,
        selector: `[id^='rc-tabs'][id$='Documentation']`,
        title: 'Documentation Tab',
        content: (
            <Typography.Paragraph>
                <p>
                    You can view and edit an entity&apos;s <strong>Documentation</strong> on this tab.
                </p>
                <p>
                    <strong>Documentation</strong> should provide descriptive information about this data asset. It can
                    also contain links to external resources.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_PROFILE_LINEAGE_ID,
        selector: `[id^='rc-tabs'][id$='Lineage']`,
        title: 'Lineage Tab',
        content: (
            <Typography.Paragraph>
                <p>
                    You can view an entity&apos;s <strong>Lineage</strong> on this tab.
                </p>
                <p>
                    Data <strong>Lineage</strong> allows you to visualize and understand both the upstream dependencies
                    and downstream consumers of this entity.
                </p>
                <p>
                    If this tab is disabled, <strong>Lineage</strong> have not been ingested for this entity.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_PROFILE_SCHEMA_ID,
        selector: `[id^='rc-tabs'][id$='Schema']`,
        title: 'Schema Tab',
        content: (
            <Typography.Paragraph>
                <p>
                    You can view a Dataset&apos;s <strong>Schema</strong> on this tab.
                </p>
                <p>
                    You can also view or add <strong>Documentation</strong>, <strong>Tags</strong>, and{' '}
                    <strong>Glossary Terms</strong> for specific columns.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_PROFILE_OWNERS_ID,
        selector: `#${ENTITY_PROFILE_OWNERS_ID}`,
        title: 'Owners',
        content: (
            <Typography.Paragraph>
                <p>
                    You can view and add <strong>Owners</strong> to this asset here.
                </p>
                <p>
                    <strong>Owners</strong> are <strong>Users</strong> or <strong>Groups</strong> who are responsible
                    for managing this asset.
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_PROFILE_TAGS_ID,
        selector: `#${ENTITY_PROFILE_TAGS_ID}`,
        title: 'Tags',
        content: (
            <Typography.Paragraph>
                <p>
                    You can view and add <strong>Tags</strong> to this asset here.
                </p>
                <p>
                    <strong>Tags</strong> are labels for organizing your data. For example, you can add a Tag to mark an
                    asset as <strong>Mission Critical</strong>.
                </p>
                <p>
                    Learn more about <strong>Tags</strong>{' '}
                    <a target="_blank" rel="noreferrer noopener" href="https://datahubproject.io/docs/tags">
                        {' '}
                        here.{' '}
                    </a>
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_PROFILE_GLOSSARY_TERMS_ID,
        selector: `#${ENTITY_PROFILE_GLOSSARY_TERMS_ID}`,
        title: 'Glossary Terms',
        content: (
            <Typography.Paragraph>
                <p>
                    You can view and add <strong>Glossary Terms</strong> to this asset here.
                </p>
                <p>
                    <strong>Glossary Terms</strong> are structured, standarized labels for organizing your
                    mission-critical data. For example, if you&apos;re marking assets containing PII fields, you might
                    add the Term <strong>Email</strong>.
                </p>
                <p>
                    Learn more about <strong>Glossary Terms</strong>{' '}
                    <a
                        target="_blank"
                        rel="noreferrer noopener"
                        href="https://datahubproject.io/docs/glossary/business-glossary"
                    >
                        {' '}
                        here.
                    </a>
                </p>
            </Typography.Paragraph>
        ),
    },
    {
        id: ENTITY_PROFILE_DOMAINS_ID,
        selector: `#${ENTITY_PROFILE_DOMAINS_ID}`,
        title: 'Domain',
        content: (
            <Typography.Paragraph>
                <p>
                    You can view and set this asset&apos;s <strong>Domain</strong> here.
                </p>
                <p>
                    <strong>Domains</strong> are collections of related data assets associated with a specific part of
                    your organization, such as the <strong>Marketing</strong> department.
                </p>
                <p>
                    Learn more about <strong>Domains</strong>{' '}
                    <a target="_blank" rel="noreferrer noopener" href="https://datahubproject.io/docs/domains">
                        {' '}
                        here.
                    </a>
                </p>
            </Typography.Paragraph>
        ),
    },
];
