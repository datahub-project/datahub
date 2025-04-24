import { Key } from 'react';

import { Assertion, DataHubSubscription, EntityType, NotificationSinkType } from '@types';

export const ChannelSelections = {
    SETTINGS: 'SETTINGS',
    SUBSCRIPTION: 'SUBSCRIPTION',
} as const;

export type ChannelSelection = (typeof ChannelSelections)[keyof typeof ChannelSelections];

export type SlackState = {
    enabled: boolean;
    channelSelection: ChannelSelection;
    subscription: {
        channel?: string;
        saveAsDefault: boolean;
    };
};

export type EmailState = {
    enabled: boolean;
    channelSelection: ChannelSelection;
    subscription: {
        channel?: string;
        saveAsDefault: boolean;
    };
};
export type State = {
    edited: boolean;
    isPersonal: boolean;
    settings: {
        sinkTypes?: NotificationSinkType[];
        slack: {
            channel?: string;
        };
        email: {
            channel?: string;
        };
    };
    notificationTypes: {
        checkedKeys: Array<Key>;
        expandedKeys: Array<Key>;
        keysWithAllFilteringCleared: Array<Key>; // ie. if user has cleared urn filters for assertions
    };
    subscribeToUpstream: boolean;
    notificationSinkTypes: Array<NotificationSinkType>;
    slack: SlackState;
    email: EmailState;
};

export type InitializeActionPayload = {
    isPersonal: boolean;
    slackSinkEnabled: boolean;
    slackSubscriptionChannel?: string;
    slackSettingsChannel?: string;
    emailSinkEnabled: boolean;
    emailSubscriptionChannel?: string;
    emailSettingsChannel?: string;
    entityType: EntityType;
    subscription?: DataHubSubscription;
    forSubResource?: {
        assertion?: Assertion;
    };
    settingsSinkTypes?: NotificationSinkType[];
};

export const ActionTypes = {
    INITIALIZE: 'INITIALIZE',
    SET_SLACK_ENABLED: 'SET_SLACK_ENABLED',
    SET_SLACK_CHANNEL_SELECTION: 'SET_SLACK_CHANNEL_SELECTION',
    SET_SLACK_SUBSCRIPTION_CHANNEL: 'SET_SLACK_SUBSCRIPTION_CHANNEL',
    SET_SLACK_SAVE_AS_DEFAULT: 'SET_SLACK_SAVE_AS_DEFAULT',
    SET_SLACK_OBJECT: 'SET_SLACK_OBJECT',
    SET_EMAIL_OBJECT: 'SET_EMAIL_OBJECT',
    SET_EMAIL_ENABLED: 'SET_EMAIL_ENABLED',
    SET_EMAIL_CHANNEL_SELECTION: 'SET_EMAIL_CHANNEL_SELECTION',
    SET_EMAIL_SUBSCRIPTION_CHANNEL: 'SET_EMAIL_SUBSCRIPTION_CHANNEL',
    SET_EMAIL_SAVE_AS_DEFAULT: 'SET_EMAIL_SAVE_AS_DEFAULT',
    SET_SUBSCRIBE_TO_UPSTREAM: 'SET_SUBSCRIBE_TO_UPSTREAM,',
    SET_NOTIFICATION_TYPES: 'SET_NOTIFICATION_TYPES',
    SET_EXPANDED_NOTIFICATION_TYPES: 'SET_EXPANDED_NOTIFICATION_TYPES',
    SET_NOTIFICATION_TYPES_WITH_FILTERS_CLEARED: 'SET_NOTIFICATION_TYPES_WITH_FILTERS_CLEARED',
} as const;

export type Action =
    | { type: typeof ActionTypes.INITIALIZE; payload: InitializeActionPayload }
    | { type: typeof ActionTypes.SET_SLACK_ENABLED; payload: boolean }
    | { type: typeof ActionTypes.SET_SLACK_CHANNEL_SELECTION; payload: ChannelSelection }
    | { type: typeof ActionTypes.SET_SLACK_SUBSCRIPTION_CHANNEL; payload: string }
    | { type: typeof ActionTypes.SET_SLACK_OBJECT; payload: SlackState }
    | { type: typeof ActionTypes.SET_EMAIL_OBJECT; payload: EmailState }
    | { type: typeof ActionTypes.SET_SLACK_SAVE_AS_DEFAULT; payload: boolean }
    | { type: typeof ActionTypes.SET_EMAIL_ENABLED; payload: boolean }
    | { type: typeof ActionTypes.SET_EMAIL_CHANNEL_SELECTION; payload: ChannelSelection }
    | { type: typeof ActionTypes.SET_EMAIL_SUBSCRIPTION_CHANNEL; payload: string }
    | { type: typeof ActionTypes.SET_EMAIL_SAVE_AS_DEFAULT; payload: boolean }
    | { type: typeof ActionTypes.SET_SUBSCRIBE_TO_UPSTREAM; payload: boolean }
    | { type: typeof ActionTypes.SET_NOTIFICATION_TYPES; payload: Array<Key> }
    | { type: typeof ActionTypes.SET_EXPANDED_NOTIFICATION_TYPES; payload: Array<Key> }
    | { type: typeof ActionTypes.SET_NOTIFICATION_TYPES_WITH_FILTERS_CLEARED; payload: Array<Key> };
