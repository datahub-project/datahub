import uniq from 'lodash/uniq';

import { ENABLE_UPSTREAM_NOTIFICATIONS } from '@app/settings/personal/notifications/constants';
import { Action, ActionTypes, ChannelSelections, State } from '@app/shared/subscribe/drawer/state/types';

import { EntityChangeType, NotificationSinkType, SubscriptionType } from '@types';

export const createInitialState = (): State => ({
    edited: false,
    isPersonal: true,
    settings: {
        slack: {},
        email: {},
        teams: {},
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
    teams: {
        enabled: false,
        channelSelection: ChannelSelections.SUBSCRIPTION,
        subscription: {
            saveAsDefault: false,
        },
        selectedResult: null,
    },
});

export const reducer = (state: State, action: Action): State => {
    switch (action.type) {
        case ActionTypes.INITIALIZE: {
            const {
                isPersonal,
                slackSinkEnabled,
                emailSinkEnabled,
                teamsSinkEnabled,
                subscription,
                forSubResource,
                slackSubscriptionChannel,
                slackSettingsChannel,
                emailSubscriptionChannel,
                emailSettingsChannel,
                teamsSubscriptionChannel,
                teamsSettingsChannel,
                teamsSettingsChannelName,
                settingsSinkTypes,
            } = action.payload;

            const relevantEntityChangeDetails = forSubResource?.assertion
                ? subscription?.entityChangeTypes?.filter(
                      (details) =>
                          !details.filter?.includeAssertions ||
                          details.filter.includeAssertions.includes(forSubResource.assertion!.urn),
                  )
                : subscription?.entityChangeTypes;

            let entityChangeTypes =
                relevantEntityChangeDetails
                    // Do not mark it as checked if this is the asset subscription view and there's filters on this type
                    ?.filter((details) => forSubResource?.assertion || !details.filter?.includeAssertions)
                    .map((details) => details.entityChangeType) ?? [];
            const notificationSinkTypes = subscription?.notificationConfig?.notificationSettings?.sinkTypes ?? [];

            if (slackSinkEnabled && !subscription) notificationSinkTypes.push(NotificationSinkType.Slack);
            if (emailSinkEnabled && !subscription) notificationSinkTypes.push(NotificationSinkType.Email);
            if (teamsSinkEnabled && !subscription) {
                notificationSinkTypes.push(NotificationSinkType.Teams);
                // Auto-select ALL notification types when Teams is enabled for the first time
                if (entityChangeTypes.length === 0) {
                    entityChangeTypes = [
                        // Entity deprecation
                        EntityChangeType.Deprecated,
                        // Assertion changes
                        EntityChangeType.AssertionFailed,
                        EntityChangeType.AssertionPassed,
                        EntityChangeType.AssertionError,
                        // Incident changes
                        EntityChangeType.IncidentRaised,
                        EntityChangeType.IncidentResolved,
                        // Schema changes
                        EntityChangeType.OperationColumnAdded,
                        EntityChangeType.OperationColumnRemoved,
                        EntityChangeType.OperationColumnModified,
                        // Ownership changes
                        EntityChangeType.OwnerAdded,
                        EntityChangeType.OwnerRemoved,
                        // Glossary term changes
                        EntityChangeType.GlossaryTermAdded,
                        EntityChangeType.GlossaryTermRemoved,
                        EntityChangeType.GlossaryTermProposed,
                        // Tag changes
                        EntityChangeType.TagAdded,
                        EntityChangeType.TagRemoved,
                        EntityChangeType.TagProposed,
                    ];
                }
            }

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

            // Teams specific logic.
            const isTeamsAndSubscriptionEnabled =
                teamsSinkEnabled && notificationSinkTypes.includes(NotificationSinkType.Teams);

            const teamsChannelSelection =
                !!teamsSettingsChannel && !teamsSubscriptionChannel
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
                    teams: {
                        channel: teamsSettingsChannel,
                        channelName: teamsSettingsChannelName,
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
                teams: {
                    ...state.teams,
                    enabled: isTeamsAndSubscriptionEnabled,
                    channelSelection: teamsChannelSelection,
                    subscription: {
                        channel: teamsSubscriptionChannel,
                        saveAsDefault: !teamsSettingsChannel && !subscription,
                    },
                    selectedResult: (() => {
                        const teamsSettings = subscription?.notificationConfig?.notificationSettings?.teamsSettings;

                        // Check if it's a user subscription
                        if (teamsSettings?.user?.azureUserId || teamsSettings?.user?.email) {
                            const teamsUser = teamsSettings.user;
                            return {
                                id: teamsUser.azureUserId || teamsUser.email!,
                                displayName: teamsUser.displayName || teamsUser.email || teamsUser.azureUserId!,
                                type: 'user' as const,
                                email: teamsUser.email || teamsUser.azureUserId!,
                            };
                        }

                        // Check if it's a channel subscription
                        if (teamsSettings?.channels?.[0]) {
                            const channel = teamsSettings.channels[0];
                            return {
                                id: channel.id,
                                displayName: channel.name || channel.id,
                                type: 'channel' as const,
                                teamName: channel.name || 'Unknown Team',
                            };
                        }

                        // Fallback: try teamsSubscriptionChannel
                        if (teamsSubscriptionChannel) {
                            return {
                                id: teamsSubscriptionChannel,
                                displayName: teamsSubscriptionChannel,
                                type: 'channel' as const,
                                teamName: 'Unknown Team',
                            };
                        }

                        return null;
                    })(),
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
        /** Teams-specific reducers */
        case ActionTypes.SET_TEAMS_ENABLED: {
            const newNotificationSinkTypes = uniq(
                action.payload
                    ? [...state.notificationSinkTypes, NotificationSinkType.Teams]
                    : state.notificationSinkTypes.filter((type) => type !== NotificationSinkType.Teams),
            );
            return {
                ...state,
                edited: true,
                notificationSinkTypes: newNotificationSinkTypes,
                teams: {
                    ...state.teams,
                    enabled: action.payload,
                },
            };
        }
        case ActionTypes.SET_TEAMS_CHANNEL_SELECTION: {
            const newSelectedResult = action.payload === ChannelSelections.SETTINGS ? null : state.teams.selectedResult;
            const newChannel =
                action.payload === ChannelSelections.SETTINGS ? undefined : state.teams.subscription.channel;

            return {
                ...state,
                edited: true,
                teams: {
                    ...state.teams,
                    channelSelection: action.payload,
                    // Clear selectedResult when switching back to SETTINGS mode to prevent using stale channel/user data
                    selectedResult: newSelectedResult,
                    subscription: {
                        ...state.teams.subscription,
                        // Clear subscription channel when switching to settings mode
                        channel: newChannel,
                    },
                },
            };
        }
        case ActionTypes.SET_TEAMS_SUBSCRIPTION_CHANNEL: {
            return {
                ...state,
                edited: true,
                teams: {
                    ...state.teams,
                    subscription: {
                        ...state.teams.subscription,
                        channel: action.payload,
                    },
                },
            };
        }
        case ActionTypes.SET_TEAMS_SAVE_AS_DEFAULT: {
            return {
                ...state,
                edited: true,
                teams: {
                    ...state.teams,
                    subscription: {
                        ...state.teams.subscription,
                        saveAsDefault: action.payload,
                    },
                },
            };
        }
        /** set whole teams object */
        case ActionTypes.SET_TEAMS_OBJECT: {
            return {
                ...state,
                edited: true,
                teams: action.payload,
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
