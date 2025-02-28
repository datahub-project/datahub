import React from 'react';
import oidcLogo from '../../../images/oidclogo.png';
import slackLogo from '../../../images/slacklogo.png';
import acrylLogo from '../../../images/acryl-dark-mark.svg';
// import teamsLogo from '../../../images/teamslogo.png';
import snowflakeLogo from '../../../images/snowflakelogo.png';
import bigqueryLogo from '../../../images/bigquerylogo.png';
import { NotificationScenarioType, NotificationSettingValue, NotificationSinkType } from '../../../types.generated';
import { SlackIntegration } from './slack/SlackIntegration';
import { OidcIntegration } from './sso/OidcIntegration';
import AcrylInstances from './acryl/AcrylInstances';
import { SnowflakeIntegration } from './integrations/SnowflakeIntegration';
import { BigQueryIntegration } from './integrations/BigQueryIntegration';
// import { TeamsIntegration } from './teams/TeamsIntegration';

/**
 * SSO
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
 * Notification Integrations
 */
const SLACK_INTEGRATION = {
    id: 'slack',
    name: 'Slack',
    img: slackLogo,
    description: 'Notify Slack channels when important things happen',
    content: <SlackIntegration />,
};

/**
 * Teams Integrations
 */
// const TEAMS_INTEGRATION = {
//     id: 'microsoft-teams',
//     name: 'Microsoft Teams',
//     img: teamsLogo,
//     description: 'Notify Teams channels when important things happen',
//     content: <TeamsIntegration />,
// };

/**
 * Acryl Instance Integrations
 */

const ACRYL_INSTANCE_INTEGRATION = {
    id: 'acryl',
    name: 'Acryl',
    img: acrylLogo,
    description: 'Integrate with another Acryl instance to enable sharing, etc.',
    content: <AcrylInstances />,
};

/**
 * Ingestion Integrations
 */
const SNOWFLAKE_INTEGRATION = {
    id: 'snowflake',
    name: 'Snowflake',
    img: snowflakeLogo,
    description: 'Manage Snowflake connections for automations',
    content: <SnowflakeIntegration />,
};

const BIGQUERY_INTEGRATION = {
    id: 'bigquery',
    name: 'BigQuery',
    img: bigqueryLogo,
    description: 'Manage BigQuery connections for automations',
    content: <BigQueryIntegration />,
};

export const SUPPORTED_INTEGRATIONS = [
    SLACK_INTEGRATION,
    // TEAMS_INTEGRATION, -- Uncheck when backend is complete.
    ACRYL_INSTANCE_INTEGRATION,
    SNOWFLAKE_INTEGRATION,
    BIGQUERY_INTEGRATION,
];

/**
 * Notifications
 */
export type PlatformNotificationOptions = {
    slackChannel: string | null;
    email: string | null;
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
        description: 'An asset is added or removed from a domain',
    },
    {
        type: NotificationScenarioType.EntityDeprecationChange,
        description: 'Deprecation status for a data asset changes',
    },
    {
        type: NotificationScenarioType.DatasetSchemaChange,
        description: 'Table fields (columns) are added, removed, or changed',
    },
];

const INGESTION_NOTIFICATIONS = [
    {
        type: NotificationScenarioType.IngestionRunChange,
        description: 'An ingestion source (integration) starts or finishes syncing',
    },
    {
        type: NotificationScenarioType.IngestionFailure,
        description: 'An ingestion source (integration) fails to complete syncing',
    },
];

const INCIDENT_NOTIFICATIONS = [
    {
        type: NotificationScenarioType.NewIncident,
        description: 'An incident is raised on an asset',
    },
    {
        type: NotificationScenarioType.IncidentStatusChange,
        description: 'An active incident is resolved for an asset',
    },
];

const ASSERTION_NOTIFICATIONS = [
    {
        type: NotificationScenarioType.AssertionStatusChange,
        description: 'An assertion passes or fails',
    },
];

const PROPOSAL_NOTIFICATIONS = [
    {
        type: NotificationScenarioType.NewProposal,
        description: 'A tag or glossary term proposal is raised',
    },
    {
        type: NotificationScenarioType.ProposalStatusChange,
        description: 'A tag or glossary term proposal is approved or denied',
    },
];

export const RECOMMENDED_PLATFORM_NOTIFICATIONS = [
    {
        title: 'Ingestion',
        notifications: INGESTION_NOTIFICATIONS,
    },
    {
        title: 'Proposals',
        notifications: PROPOSAL_NOTIFICATIONS,
    },
];

export const NON_RECOMMENDED_PLATFORM_NOTIFICATIONS = [
    {
        title: 'Changes',
        notifications: CHANGE_NOTIFICATIONS,
    },
    {
        title: 'Assertions',
        notifications: ASSERTION_NOTIFICATIONS,
    },
    {
        title: 'Incidents',
        notifications: INCIDENT_NOTIFICATIONS,
    },
];

export type NotificationSink = {
    id: string;
    name: string;
    img?: any;
    options: boolean;
};

export const SLACK_SINK = {
    type: NotificationSinkType.Slack,
    id: SLACK_INTEGRATION.id,
    name: SLACK_INTEGRATION.name,
    img: SLACK_INTEGRATION.img,
    options: true,
};

export const EMAIL_SINK = {
    type: NotificationSinkType.Email,
    id: 'email',
    name: 'Email',
    img: undefined,
    options: true,
};

export const NOTIFICATION_SINKS = [SLACK_SINK, EMAIL_SINK];

export type FormattedNotificationSetting = {
    type: NotificationScenarioType;
    value: NotificationSettingValue;
    params: Map<string, string>;
};
