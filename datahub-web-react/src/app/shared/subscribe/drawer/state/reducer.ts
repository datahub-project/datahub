import uniq from 'lodash/uniq';
import { NotificationSinkType, SubscriptionType } from '../../../../../types.generated';
import { ENABLE_UPSTREAM_NOTIFICATIONS } from '../../../../settings/personal/notifications/constants';
import { Action, ActionTypes, ChannelSelections, State } from './types';

export const createInitialState = (): State => ({
    edited: false,
    isPersonal: true,
    settings: {
        slack: {},
    },
    notificationTypes: {
        checkedKeys: [],
        expandedKeys: [],
    },
    subscribeToUpstream: false,
    notificationSinkTypes: [],
    slack: {
        enabled: false,
        channelSelection: ChannelSelections.SUBSCRIPTION,
        subscription: {
            saveAsDefault: false,
        },
    },
});

export const reducer = (state: State, action: Action): State => {
    switch (action.type) {
        case ActionTypes.INITIALIZE: {
            const {
                isPersonal,
                slackSinkEnabled,
                subscription,
                subscriptionChannel,
                settingsChannel,
                settingsSinkTypes,
            } = action.payload;

            const entityChangeTypes =
                subscription?.entityChangeTypes.map((changeType) => changeType.entityChangeType) ?? [];
            const notificationSinkTypes = subscription?.notificationConfig?.notificationSettings?.sinkTypes ?? [];

            if (slackSinkEnabled && !subscription) notificationSinkTypes.push(NotificationSinkType.Slack);

            const isSlackAndSubscriptionEnabled =
                slackSinkEnabled && notificationSinkTypes.includes(NotificationSinkType.Slack);
            const hasUpstreamSubscription =
                ENABLE_UPSTREAM_NOTIFICATIONS &&
                !!subscription?.subscriptionTypes?.includes(SubscriptionType.UpstreamEntityChange);
            const channelSelection =
                !!settingsChannel && !subscriptionChannel ? ChannelSelections.SETTINGS : ChannelSelections.SUBSCRIPTION;

            return {
                ...state,
                isPersonal,
                edited: !subscription,
                settings: {
                    sinkTypes: settingsSinkTypes,
                    slack: {
                        channel: settingsChannel,
                    },
                },
                notificationTypes: {
                    checkedKeys: entityChangeTypes,
                    expandedKeys: [],
                },
                subscribeToUpstream: hasUpstreamSubscription,
                notificationSinkTypes,
                slack: {
                    ...state.slack,
                    enabled: isSlackAndSubscriptionEnabled,
                    channelSelection,
                    subscription: {
                        channel: subscriptionChannel,
                        saveAsDefault: !settingsChannel && !subscription,
                    },
                },
            };
        }
        case ActionTypes.SET_SLACK_ENABLED: {
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
        case ActionTypes.SET_CHANNEL_SELECTION: {
            return {
                ...state,
                edited: true,
                slack: {
                    ...state.slack,
                    channelSelection: action.payload,
                    subscription: {
                        ...state.slack.subscription,
                        channel:
                            action.payload === ChannelSelections.SETTINGS
                                ? undefined
                                : state.slack.subscription.channel,
                        saveAsDefault: !state.settings.slack.channel,
                    },
                },
            };
        }
        case ActionTypes.SET_SUBSCRIPTION_CHANNEL: {
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
        case ActionTypes.SET_SAVE_AS_DEFAULT: {
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
        case ActionTypes.SET_SUBSCRIBE_TO_UPSTREAM: {
            return {
                ...state,
                edited: true,
                subscribeToUpstream: action.payload,
            };
        }
        case ActionTypes.SET_NOTIFICATION_TYPES: {
            return {
                ...state,
                edited: true,
                notificationTypes: {
                    ...state.notificationTypes,
                    checkedKeys: action.payload,
                },
            };
        }
        case ActionTypes.SET_EXPANDED_NOTIFICATION_TYPES: {
            return {
                ...state,
                edited: true,
                notificationTypes: {
                    ...state.notificationTypes,
                    expandedKeys: action.payload,
                },
            };
        }
        default: {
            return state;
        }
    }
};
