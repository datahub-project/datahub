import { NotificationScenarioType } from '@src/types.generated';

/**
 * Notifications for users & groups.
 */
export type ActorNotificationOptions = {
    slackChannel: string | null;
    email: string | null;
};

const GROUP_PROPOSAL_NOTIFICATIONS = [
    {
        type: NotificationScenarioType.NewProposal,
        description: 'This group is assigned to a new change proposal',
    },
    {
        type: NotificationScenarioType.ProposalStatusChange,
        description: 'A proposal this group is assigned to is approved or denied',
    },
];

const GROUP_ACTION_WORKFLOW_FORM_REQUEST_NOTIFICATIONS = [
    {
        type: NotificationScenarioType.NewActionWorkflowFormRequest,
        description: 'This group is assigned to review a new workflow form request',
    },
];

const USER_PROPOSAL_NOTIFICATIONS = [
    {
        type: NotificationScenarioType.ProposerProposalStatusChange,
        description: 'A proposal you raised is approved or denied',
    },
    {
        type: NotificationScenarioType.NewProposal,
        description: 'You are assigned to a new change proposal',
    },
    {
        type: NotificationScenarioType.ProposalStatusChange,
        description: 'A proposal you are assigned to is approved or denied',
    },
];

const USER_COMPLIANCE_FORMS_NOTIFICATIONS = [
    {
        type: NotificationScenarioType.ComplianceFormPublish,
        description: 'A form to which you are assigned is published',
        default: true,
    },
];

const USER_ACTION_WORKFLOW_FORM_NOTIFICATIONS = [
    {
        type: NotificationScenarioType.NewActionWorkflowFormRequest,
        description: 'You are assigned to review a new workflow form request',
    },
    {
        type: NotificationScenarioType.RequesterActionWorkflowFormRequestStatusChange,
        description: 'A workflow form request you raised is approved or denied',
    },
];

export const USER_PROPOSAL_NOTIFICATIONS_GROUP = {
    title: 'Proposals',
    notifications: USER_PROPOSAL_NOTIFICATIONS,
};

export const USER_COMPLIANCE_FORMS_NOTIFICATIONS_GROUP = {
    title: 'Compliance Forms',
    notifications: USER_COMPLIANCE_FORMS_NOTIFICATIONS,
};

export const GROUP_PROPOSAL_NOTIFICATIONS_GROUP = {
    title: 'Proposals',
    notifications: GROUP_PROPOSAL_NOTIFICATIONS,
};

export const USER_ACTION_WORKFLOW_FORM_REQUEST_NOTIFICATIONS_GROUP = {
    title: 'Workflows',
    notifications: USER_ACTION_WORKFLOW_FORM_NOTIFICATIONS,
};

export const GROUP_ACTION_WORKFLOW_FORM_REQUEST_NOTIFICATIONS_GROUP = {
    title: 'Workflows',
    notifications: GROUP_ACTION_WORKFLOW_FORM_REQUEST_NOTIFICATIONS,
};

const USER_COMMUNITY_UPDATES_NOTIFICATIONS = [
    {
        type: NotificationScenarioType.DataHubCommunityUpdates,
        description: 'Community updates, announcements, and news about DataHub',
        default: true, // Default enabled - users auto-subscribed
    },
];

export const USER_COMMUNITY_UPDATES_NOTIFICATIONS_GROUP = {
    title: 'Community Updates',
    notifications: USER_COMMUNITY_UPDATES_NOTIFICATIONS,
};
