import { Key } from 'react';
import { DataHubSubscription, EntityType, NotificationSinkType } from '../../../../../types.generated';

// todo - declare a FormState type
// in our context we have both a base, and a modified FormState
// modiifed is a straight copy, with overrides, and a boolean for edited?
// maybe we just keep a bool and can reset it

// what about the gql values, do we even need that in state?

export type ChannelSelection = 'settings' | 'subscription';

export type State = {
    edited: boolean;
    isPersonal: boolean;
    checkedKeys: Array<Key>;
    subscribeToUpstream: boolean;
    notificationSinkTypes: Array<NotificationSinkType>;
    slack: {
        enabled: boolean;
        channelSelection: ChannelSelection;
        settings: {
            channel?: string;
        };
        subscription: {
            channel?: string;
            saveAsDefault: boolean;
        };
    };
};

export type InitializeActionPayload = {
    slackSinkEnabled: boolean;
    entityType: EntityType;
    subscription?: DataHubSubscription;
    subscriptionChannel?: string;
    settingsChannel?: string;
};

export const ActionTypes = {
    INITIALIZE: 'INITIALIZE',
    SET_SLACK_ENABLED: 'SET_SLACK_ENABLED',
    SET_CHANNEL_SELECTION: 'SET_CHANNEL_SELECTION',
    SET_SUBSCRIPTION_CHANNEL: 'SET_SUBSCRIPTION_CHANNEL',
    SET_SAVE_AS_DEFAULT: 'SET_SAVE_AS_DEFAULT',
    SET_CHECKED_KEYS: 'SET_CHECKED_KEYS',
    SET_SUBSCRIBE_TO_UPSTREAM: 'SET_SUBSCRIBE_TO_UPSTREAM,',
} as const;

export type Action =
    | {
          type: typeof ActionTypes.INITIALIZE;
          payload: InitializeActionPayload;
      }
    | { type: typeof ActionTypes.SET_SLACK_ENABLED; payload: boolean }
    | { type: typeof ActionTypes.SET_CHANNEL_SELECTION; payload: ChannelSelection }
    | { type: typeof ActionTypes.SET_SUBSCRIPTION_CHANNEL; payload: string }
    | { type: typeof ActionTypes.SET_SAVE_AS_DEFAULT; payload: boolean }
    | { type: typeof ActionTypes.SET_CHECKED_KEYS; payload: Array<Key> }
    | { type: typeof ActionTypes.SET_SUBSCRIBE_TO_UPSTREAM; payload: boolean };
