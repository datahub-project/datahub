import { Key } from 'react';
import { DataHubSubscription, EntityType, NotificationSinkType } from '../../../../../types.generated';

export const ChannelSelections = {
    SETTINGS: 'SETTINGS',
    SUBSCRIPTION: 'SUBSCRIPTION',
} as const;

export type ChannelSelection = typeof ChannelSelections[keyof typeof ChannelSelections];

export type State = {
    edited: boolean;
    isPersonal: boolean;
    settings: {
        sinkTypes?: NotificationSinkType[];
        slack: {
            channel?: string;
        };
    };
    notificationTypes: {
        checkedKeys: Array<Key>;
        expandedKeys: Array<Key>;
    };
    subscribeToUpstream: boolean;
    notificationSinkTypes: Array<NotificationSinkType>;
    slack: {
        enabled: boolean;
        channelSelection: ChannelSelection;
        subscription: {
            channel?: string;
            saveAsDefault: boolean;
        };
    };
    // other sink types go here
};

export type InitializeActionPayload = {
    isPersonal: boolean;
    slackSinkEnabled: boolean;
    entityType: EntityType;
    subscription?: DataHubSubscription;
    subscriptionChannel?: string;
    settingsChannel?: string;
    settingsSinkTypes?: NotificationSinkType[];
};

export const ActionTypes = {
    INITIALIZE: 'INITIALIZE',
    SET_SLACK_ENABLED: 'SET_SLACK_ENABLED',
    SET_CHANNEL_SELECTION: 'SET_CHANNEL_SELECTION',
    SET_SUBSCRIPTION_CHANNEL: 'SET_SUBSCRIPTION_CHANNEL',
    SET_SAVE_AS_DEFAULT: 'SET_SAVE_AS_DEFAULT',
    SET_SUBSCRIBE_TO_UPSTREAM: 'SET_SUBSCRIBE_TO_UPSTREAM,',
    SET_NOTIFICATION_TYPES: 'SET_NOTIFICATION_TYPES',
    SET_EXPANDED_NOTIFICATION_TYPES: 'SET_EXPANDED_NOTIFICATION_TYPES',
} as const;

export type Action =
    | { type: typeof ActionTypes.INITIALIZE; payload: InitializeActionPayload }
    | { type: typeof ActionTypes.SET_SLACK_ENABLED; payload: boolean }
    | { type: typeof ActionTypes.SET_CHANNEL_SELECTION; payload: ChannelSelection }
    | { type: typeof ActionTypes.SET_SUBSCRIPTION_CHANNEL; payload: string }
    | { type: typeof ActionTypes.SET_SAVE_AS_DEFAULT; payload: boolean }
    | { type: typeof ActionTypes.SET_SUBSCRIBE_TO_UPSTREAM; payload: boolean }
    | { type: typeof ActionTypes.SET_NOTIFICATION_TYPES; payload: Array<Key> }
    | { type: typeof ActionTypes.SET_EXPANDED_NOTIFICATION_TYPES; payload: Array<Key> };
