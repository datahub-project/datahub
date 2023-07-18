import uniq from 'lodash/uniq';
import { NotificationSinkType, SubscriptionType } from '../../../../../types.generated';
import { ENABLE_UPSTREAM_NOTIFICATIONS } from '../../../../settings/personal/notifications/constants';
import { getDefaultCheckedKeys } from '../utils';
import { Action, State } from './types';

// todo - make this look more form-like, for ex, maybe name the ui controls involved here?
export const createInitialState = (isPersonal: boolean): State => ({
    edited: false,
    isPersonal,
    checkedKeys: [],
    subscribeToUpstream: false,
    notificationSinkTypes: [],
    slack: {
        enabled: false,
        channelSelection: 'subscription',
        settings: {},
        subscription: {
            saveAsDefault: false,
        },
    },
});

export const reducer = (state: State, action: Action): State => {
    switch (action.type) {
        case 'initialize': {
            const { slackSinkEnabled, entityType, subscription, subscriptionChannel, settingsChannel } = action.payload;

            const entityChangeTypes = subscription?.entityChangeTypes ?? getDefaultCheckedKeys(entityType);
            const notificationSinkTypes = subscription?.notificationConfig?.sinkTypes ?? [];
            const isSlackAndSubscriptionEnabled =
                slackSinkEnabled && notificationSinkTypes.includes(NotificationSinkType.Slack);
            const hasUpstreamSubscription =
                ENABLE_UPSTREAM_NOTIFICATIONS &&
                !!subscription?.subscriptionTypes?.includes(SubscriptionType.UpstreamEntityChange);
            const channelSelection = !!settingsChannel && !subscriptionChannel ? 'settings' : 'subscription';

            return {
                ...state,
                edited: !subscription,
                checkedKeys: entityChangeTypes,
                subscribeToUpstream: hasUpstreamSubscription,
                notificationSinkTypes,
                slack: {
                    ...state.slack,
                    enabled: isSlackAndSubscriptionEnabled,
                    channelSelection,
                    settings: {
                        channel: settingsChannel,
                    },
                    subscription: {
                        channel: subscriptionChannel,
                        saveAsDefault: !settingsChannel,
                    },
                },
            };
        }
        case 'setSlackEnabled': {
            const newNotificationSinkTypes = uniq(
                action.payload
                    ? [...state.notificationSinkTypes, NotificationSinkType.Slack]
                    : state.notificationSinkTypes.filter((sinkType) => sinkType !== NotificationSinkType.Slack),
            ).sort((a, b) => a.localeCompare(b));

            return {
                ...state,
                edited: true,
                notificationSinkTypes: newNotificationSinkTypes,
                slack: {
                    ...state.slack,
                    enabled: action.payload,
                },
            };
        }
        case 'setChannelSelection': {
            return {
                ...state,
                edited: true,
                slack: {
                    ...state.slack,
                    channelSelection: action.payload,
                    subscription: {
                        ...state.slack.subscription,
                        channel: action.payload ? undefined : state.slack.subscription.channel,
                        saveAsDefault: !state.slack.settings.channel,
                    },
                },
            };
        }
        case 'setSubscriptionChannel': {
            return {
                ...state,
                edited: true,
                slack: {
                    ...state.slack,
                    subscription: {
                        ...state.slack.subscription,
                        channel: action.payload,
                    },
                },
            };
        }
        case 'setSaveAsDefault': {
            return {
                ...state,
                edited: true,
                slack: {
                    ...state.slack,
                    subscription: {
                        ...state.slack.subscription,
                        saveAsDefault: action.payload,
                    },
                },
            };
        }
        case 'setCheckedKeys': {
            return {
                ...state,
                edited: true,
                checkedKeys: action.payload,
            };
        }
        case 'setSubscribeToUpstream': {
            return {
                ...state,
                edited: true,
                subscribeToUpstream: action.payload,
            };
        }
        default: {
            return state;
        }
    }
};
