import { Text } from '@components';
import React from 'react';
import { Trans } from 'react-i18next';

import { OnboardingStep } from '@app/onboarding/OnboardingStep';

// Entity profile tabs. Note that the 'rc-tab' prefix for the ID is added by the antd library and may change in the future.
const ENTITY_PROFILE_ENTITIES_ID = 'entity-profile-entities';
export const ENTITY_PROFILE_PROPERTIES_ID = 'entity-profile-properties';
const ENTITY_PROFILE_DOCUMENTATION_ID = 'entity-profile-documentation';
export const ENTITY_PROFILE_LINEAGE_ID = 'entity-profile-lineage';
const ENTITY_PROFILE_SCHEMA_ID = 'entity-profile-schema';

// Entity profile sidebar
export const ENTITY_PROFILE_OWNERS_ID = 'entity-profile-owners';
export const ENTITY_PROFILE_TAGS_ID = 'entity-profile-tags';
export const ENTITY_PROFILE_GLOSSARY_TERMS_ID = 'entity-profile-glossary-terms';
export const ENTITY_PROFILE_DOMAINS_ID = 'entity-profile-domains';
export const ENTITY_PROFILE_V2_SIDEBAR_ID = 'entity-profile-v2-sidebar';

export const EntityProfileOnboardingConfig: OnboardingStep[] = [
    {
        id: ENTITY_PROFILE_ENTITIES_ID,
        selector: `[id^='rc-tabs'][id$='Entities']`,
        title: <Trans i18nKey="onboarding:entityProfile.entitiesTabTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans
                        i18nKey="onboarding:entityProfile.entitiesTabDescription"
                        components={{ bold: <strong /> }}
                    />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_PROFILE_PROPERTIES_ID,
        selector: `[id^='rc-tabs'][id$='Properties']`,
        title: <Trans i18nKey="onboarding:entityProfile.propertiesTabTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans
                        i18nKey="onboarding:entityProfile.propertiesTabDescription1"
                        components={{ bold: <strong /> }}
                    />
                </p>
                <p>
                    <Trans
                        i18nKey="onboarding:entityProfile.propertiesTabDescription2"
                        components={{ bold: <strong /> }}
                    />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_PROFILE_DOCUMENTATION_ID,
        selector: `[id^='rc-tabs'][id$='Documentation']`,
        title: <Trans i18nKey="onboarding:entityProfile.documentationTabTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans
                        i18nKey="onboarding:entityProfile.documentationTabDescription1"
                        components={{ bold: <strong /> }}
                    />
                </p>
                <p>
                    <Trans
                        i18nKey="onboarding:entityProfile.documentationTabDescription2"
                        components={{ bold: <strong /> }}
                    />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_PROFILE_LINEAGE_ID,
        selector: `[id^='rc-tabs'][id$='Lineage']`,
        title: <Trans i18nKey="onboarding:entityProfile.lineageTabTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans
                        i18nKey="onboarding:entityProfile.lineageTabDescription1"
                        components={{ bold: <strong /> }}
                    />
                </p>
                <p>
                    <Trans i18nKey="onboarding:lineageGraph.dataLineageDescription" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans
                        i18nKey="onboarding:entityProfile.lineageTabDescription3"
                        components={{ bold: <strong /> }}
                    />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_PROFILE_SCHEMA_ID,
        selector: `[id^='rc-tabs'][id$='Schema']`,
        title: <Trans i18nKey="onboarding:entityProfile.schemaTabTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:entityProfile.schemaTabDescription1" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:entityProfile.schemaTabDescription2" components={{ bold: <strong /> }} />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_PROFILE_OWNERS_ID,
        selector: `#${ENTITY_PROFILE_OWNERS_ID}`,
        title: <Trans i18nKey="common.labels:owners" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:entityProfile.ownersTabDescription1" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:entityProfile.ownersTabDescription2" components={{ bold: <strong /> }} />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_PROFILE_TAGS_ID,
        selector: `#${ENTITY_PROFILE_TAGS_ID}`,
        title: <Trans i18nKey="common.labels:tags" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:entityProfile.tagsTabDescription1" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:entityProfile.tagsTabDescription2" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans
                        i18nKey="onboarding:entityProfile.tagsLearnMore"
                        components={{
                            bold: <strong />,
                            anchor: (
                                // eslint-disable-next-line jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label
                                <a
                                    target="_blank"
                                    rel="noreferrer noopener"
                                    href="https://docs.datahub.com/docs/tags"
                                />
                            ),
                        }}
                    />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_PROFILE_GLOSSARY_TERMS_ID,
        selector: `#${ENTITY_PROFILE_GLOSSARY_TERMS_ID}`,
        title: <Trans i18nKey="onboarding:entityProfile.glossaryTermsTabTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans
                        i18nKey="onboarding:entityProfile.glossaryTermsTabDescription1"
                        components={{ bold: <strong /> }}
                    />
                </p>
                <p>
                    <Trans
                        i18nKey="onboarding:entityProfile.glossaryTermsTabDescription2"
                        components={{ bold: <strong /> }}
                    />
                </p>
                <p>
                    <Trans
                        i18nKey="onboarding:entityProfile.glossaryTermsLearnMore"
                        components={{
                            bold: <strong />,
                            anchor: (
                                // eslint-disable-next-line jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label
                                <a
                                    target="_blank"
                                    rel="noreferrer noopener"
                                    href="https://docs.datahub.com/docs/glossary/business-glossary"
                                />
                            ),
                        }}
                    />
                </p>
            </Text>
        ),
    },
    {
        id: ENTITY_PROFILE_DOMAINS_ID,
        selector: `#${ENTITY_PROFILE_DOMAINS_ID}`,
        title: <Trans i18nKey="onboarding:entityProfile.domainTabTitle" />,
        content: (
            <Text type="div" size="md">
                <p>
                    <Trans i18nKey="onboarding:entityProfile.domainTabDescription1" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans i18nKey="onboarding:domains.introDescription" components={{ bold: <strong /> }} />
                </p>
                <p>
                    <Trans
                        i18nKey="onboarding:entityProfile.domainLearnMore"
                        components={{
                            bold: <strong />,
                            anchor: (
                                // eslint-disable-next-line jsx-a11y/anchor-has-content, jsx-a11y/control-has-associated-label
                                <a
                                    target="_blank"
                                    rel="noreferrer noopener"
                                    href="https://docs.datahub.com/docs/domains"
                                />
                            ),
                        }}
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
                    <Trans i18nKey="onboarding:entityProfile.sidebarDescription" />
                </p>
            </Text>
        ),
    },
];
