import React from 'react';
import oidcLogo from '../../../images/oidclogo.png';
import slackLogo from '../../../images/slacklogo.png';
import { NotificationScenarioType, NotificationSettingValue } from '../../../types.generated';
import { SlackIntegration } from './slack/SlackIntegration';
import { OidcIntegration } from './sso/OidcIntegration';

/**
 * SSO Integrations
 */

const OIDC_INTEGRATION = {
    id: 'oidc',
    name: 'OIDC',
    img: oidcLogo,
    description: 'Integrate DataHub with your OIDC SSO provider ',
    content: <OidcIntegration />,
};

export const SUPPORTED_SSO_INTEGRATIONS = [OIDC_INTEGRATION];
/**
 * Integrations
 */

const SLACK_INTEGRATION = {
    id: 'slack',
    name: 'Slack',
    img: slackLogo,
    description: 'Notify Slack channels when important things happen',
    content: <SlackIntegration />,
};

export const SUPPORTED_INTEGRATIONS = [SLACK_INTEGRATION];

/**
 * Notifications
 */

export type PlatformNotificationOptions = {
    slackChannel: string | undefined;
};

const CHANGE_NOTIFICATIONS = [
    {
        type: NotificationScenarioType.EntityOwnerChange,
        description: 'An owner is added or removed from a data asset',
    },
    {
        type: NotificationScenarioType.EntityTagChange,
        description: 'A tag is added or removed from a data asset',
    },
    {
        type: NotificationScenarioType.EntityGlossaryTermChange,
        description: 'A glossary term is added or removed from a data asset',
    },
    {
        type: NotificationScenarioType.EntityDomainChange,
        description: 'A domain is added or removed from a data asset',
    },
    {
        type: NotificationScenarioType.EntityDeprecationChange,
        description: 'Deprecation status for a data asset changes',
    },
    {
        type: NotificationScenarioType.DatasetSchemaChange,
        description: 'Schema fields are added to or removed from a dataset',
    },
];

const INGESTION_NOTIFICATIONS = [
    {
        type: NotificationScenarioType.IngestionRunChange,
        description: 'An ingestion source execution starts or finishes',
    },
];

const INCIDENT_NOTIFICATIONS = [
    {
        type: NotificationScenarioType.NewIncident,
        description: 'An incident is raised',
    },
    {
        type: NotificationScenarioType.IncidentStatusChange,
        description: 'An incident is resolved',
    },
];

const PROPOSAL_NOTIFICATIONS = [
    {
        type: NotificationScenarioType.NewProposal,
        description: 'A tag or glossary term proposal is raised',
    },
    {
        type: NotificationScenarioType.ProposalStatusChange,
        description: 'A tag or glossary term proposal is completed',
    },
];

export const NOTIFICATION_GROUPS = [
    {
        title: 'Changes',
        notifications: CHANGE_NOTIFICATIONS,
    },
    {
        title: 'Ingestion',
        notifications: INGESTION_NOTIFICATIONS,
    },
    {
        title: 'Incidents',
        notifications: INCIDENT_NOTIFICATIONS,
    },
    {
        title: 'Proposals',
        notifications: PROPOSAL_NOTIFICATIONS,
    },
];

export type NotificationSink = {
    id: string;
    name: string;
    img: any;
    options: boolean;
};

export const SLACK_SINK = {
    id: SLACK_INTEGRATION.id,
    name: SLACK_INTEGRATION.name,
    img: SLACK_INTEGRATION.img,
    options: true,
};

export const NOTIFICATION_SINKS = [SLACK_SINK];

export type FormattedNotificationSetting = {
    type: NotificationScenarioType;
    value: NotificationSettingValue;
    params: Map<string, string>;
};
