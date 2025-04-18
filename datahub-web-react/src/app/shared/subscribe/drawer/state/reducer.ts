import uniq from 'lodash/uniq';

import { ENABLE_UPSTREAM_NOTIFICATIONS } from '@app/settings/personal/notifications/constants';
import { Action, ActionTypes, ChannelSelections, State } from '@app/shared/subscribe/drawer/state/types';

import { NotificationSinkType, SubscriptionType } from '@types';

export const createInitialState = (): State => ({
    edited: false,
    isPersonal: true,
    settings: {
        slack: {},
        email: {},
    },
    notificationTypes: {
        checkedKeys: [],
        expandedKeys: [],
        keysWithAllFilteringCleared: [],
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
    email: {
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
                emailSinkEnabled,
                subscription,
                forSubResource,
                slackSubscriptionChannel,
                slackSettingsChannel,
                emailSubscriptionChannel,
                emailSettingsChannel,
                settingsSinkTypes,
            } = action.payload;

            const relevantEntityChangeDetails = forSubResource?.assertion
                ? subscription?.entityChangeTypes.filter(
                      (details) =>
                          !details.filter?.includeAssertions ||
                          details.filter.includeAssertions.includes(forSubResource.assertion!.urn),
                  )
                : subscription?.entityChangeTypes;

            const entityChangeTypes =
                relevantEntityChangeDetails
                    // Do not mark it as checked if this is the asset subscription view and there's filters on this type
                    ?.filter((details) => forSubResource?.assertion || !details.filter?.includeAssertions)
                    .map((details) => details.entityChangeType) ?? [];
            const notificationSinkTypes = subscription?.notificationConfig?.notificationSettings?.sinkTypes ?? [];

            if (slackSinkEnabled && !subscription) notificationSinkTypes.push(NotificationSinkType.Slack);
            if (emailSinkEnabled && !subscription) notificationSinkTypes.push(NotificationSinkType.Email);

            // Slack specific logic.
            const isSlackAndSubscriptionEnabled =
                slackSinkEnabled && notificationSinkTypes.includes(NotificationSinkType.Slack);

            const slackChannelSelection =
                !!slackSettingsChannel && !slackSubscriptionChannel
                    ? ChannelSelections.SETTINGS
                    : ChannelSelections.SUBSCRIPTION;

            // Email specific logic.
            const isEmailAndSubscriptionEnabled =
                emailSinkEnabled && notificationSinkTypes.includes(NotificationSinkType.Email);

            const emailChannelSelection =
                !!emailSettingsChannel && !emailSubscriptionChannel
                    ? ChannelSelections.SETTINGS
                    : ChannelSelections.SUBSCRIPTION;

            // TODO: once we implement upstream subscriptions.
            const hasUpstreamSubscription =
                ENABLE_UPSTREAM_NOTIFICATIONS &&
                !!subscription?.subscriptionTypes?.includes(SubscriptionType.UpstreamEntityChange);

            return {
                ...state,
                isPersonal,
                edited: !subscription,
                settings: {
                    sinkTypes: settingsSinkTypes,
                    slack: {
                        channel: slackSettingsChannel,
                    },
                    email: {
                        channel: emailSettingsChannel,
                    },
                },
                notificationTypes: {
                    checkedKeys: entityChangeTypes,
                    expandedKeys: [],
                    keysWithAllFilteringCleared: [],
                },
                subscribeToUpstream: hasUpstreamSubscription,
                notificationSinkTypes,
                slack: {
                    ...state.slack,
                    enabled: isSlackAndSubscriptionEnabled,
                    channelSelection: slackChannelSelection,
                    subscription: {
                        channel: slackSubscriptionChannel,
                        saveAsDefault: !slackSettingsChannel && !subscription,
                    },
                },
                email: {
                    ...state.email,
                    enabled: isEmailAndSubscriptionEnabled,
                    channelSelection: emailChannelSelection,
                    subscription: {
                        channel: emailSubscriptionChannel,
                        saveAsDefault: !emailSettingsChannel && !subscription,
                    },
                },
            };
        }
        /** Slack-specific reducers */
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
        case ActionTypes.SET_SLACK_CHANNEL_SELECTION: {
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
        case ActionTypes.SET_SLACK_SUBSCRIPTION_CHANNEL: {
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
        case ActionTypes.SET_SLACK_SAVE_AS_DEFAULT: {
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
        /** set whole slack object */
        case ActionTypes.SET_SLACK_OBJECT: {
            return {
                ...state,
                edited: true,
                slack: action.payload,
            };
        }
        /** Email-specific reducers */
        case ActionTypes.SET_EMAIL_ENABLED: {
            const newNotificationSinkTypes = uniq(
                action.payload
                    ? [...state.notificationSinkTypes, NotificationSinkType.Email]
                    : state.notificationSinkTypes.filter((sinkType) => sinkType !== NotificationSinkType.Email),
            ).sort((a, b) => a.localeCompare(b));

            return {
                ...state,
                edited: true,
                notificationSinkTypes: newNotificationSinkTypes,
                email: {
                    ...state.email,
                    enabled: action.payload,
                },
            };
        }
        case ActionTypes.SET_EMAIL_CHANNEL_SELECTION: {
            return {
                ...state,
                edited: true,
                email: {
                    ...state.email,
                    channelSelection: action.payload,
                    subscription: {
                        ...state.email.subscription,
                        channel:
                            action.payload === ChannelSelections.SETTINGS
                                ? undefined
                                : state.email.subscription.channel,
                        saveAsDefault: !state.settings.email.channel,
                    },
                },
            };
        }
        case ActionTypes.SET_EMAIL_SUBSCRIPTION_CHANNEL: {
            return {
                ...state,
                edited: true,
                email: {
                    ...state.email,
                    subscription: {
                        ...state.email.subscription,
                        channel: action.payload,
                    },
                },
            };
        }
        case ActionTypes.SET_EMAIL_SAVE_AS_DEFAULT: {
            return {
                ...state,
                edited: true,
                email: {
                    ...state.email,
                    subscription: {
                        ...state.email.subscription,
                        saveAsDefault: action.payload,
                    },
                },
            };
        }
        /** set whole email object */
        case ActionTypes.SET_EMAIL_OBJECT: {
            return {
                ...state,
                edited: true,
                email: action.payload,
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
        case ActionTypes.SET_NOTIFICATION_TYPES_WITH_FILTERS_CLEARED: {
            return {
                ...state,
                edited: true,
                notificationTypes: {
                    ...state.notificationTypes,
                    keysWithAllFilteringCleared: action.payload,
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
