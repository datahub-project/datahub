import { createInitialState, reducer } from '@app/shared/subscribe/drawer/state/reducer';
import { ActionTypes, ChannelSelections } from '@app/shared/subscribe/drawer/state/types';

import { DataHubSubscription, EntityChangeType, EntityType, NotificationSinkType } from '@types';

const entityType = EntityType.Dataset;
const slackSubscription: Partial<DataHubSubscription> = {
    entityChangeTypes: [],
    notificationConfig: {
        notificationSettings: {
            sinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
            slackSettings: {},
            emailSettings: undefined,
        },
    },
};

const getInitializedState = ({
    slackSinkEnabled,
    emailSinkEnabled,
    teamsSinkEnabled,
    subscription,
    slackSettingsChannel,
    slackSubscriptionChannel,
    emailSettingsChannel,
    emailSubscriptionChannel,
    teamsSettingsChannel,
    teamsSettingsChannelName,
    teamsSubscriptionChannel,
    settingsSinkTypes,
}: {
    slackSinkEnabled: boolean;
    emailSinkEnabled: boolean;
    teamsSinkEnabled: boolean;
    subscription?: Partial<DataHubSubscription>;
    slackSettingsChannel?: string;
    slackSubscriptionChannel?: string;
    emailSettingsChannel?: string;
    emailSubscriptionChannel?: string;
    teamsSettingsChannel?: string;
    teamsSettingsChannelName?: string;
    teamsSubscriptionChannel?: string;
    settingsSinkTypes?: NotificationSinkType[];
}) => {
    const state = createInitialState();
    const action = {
        type: ActionTypes.INITIALIZE,
        payload: {
            isPersonal: true,
            slackSinkEnabled,
            emailSinkEnabled,
            teamsSinkEnabled,
            entityType,
            subscription: subscription as DataHubSubscription,
            slackSubscriptionChannel,
            slackSettingsChannel,
            emailSettingsChannel,
            emailSubscriptionChannel,
            teamsSettingsChannel,
            teamsSettingsChannelName,
            teamsSubscriptionChannel,
            settingsSinkTypes,
        },
    };

    return reducer(state, action);
};

describe('reducer', () => {
    describe(`${ActionTypes.INITIALIZE} with slack globally disabled`, () => {
        it('should set state', () => {
            const newState = getInitializedState({
                slackSinkEnabled: false,
                emailSinkEnabled: false,
                teamsSinkEnabled: false,
                subscription: undefined,
                slackSettingsChannel: undefined,
                slackSubscriptionChannel: undefined,
                emailSettingsChannel: undefined,
                emailSubscriptionChannel: undefined,
                teamsSettingsChannel: undefined,
                teamsSubscriptionChannel: undefined,
                settingsSinkTypes: [],
            });

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: [],
                    expandedKeys: [],
                    keysWithAllFilteringCleared: [],
                },
                settings: {
                    sinkTypes: [],
                    slack: {
                        channel: undefined,
                    },
                    email: {
                        channel: undefined,
                    },
                    teams: {
                        channel: undefined,
                        channelName: undefined,
                    },
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [],
                slack: {
                    enabled: false,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: true,
                    },
                },
                email: {
                    enabled: false,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: true,
                    },
                },
                teams: {
                    enabled: false,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: true,
                    },
                    selectedResult: null,
                },
            });
        });
    });

    describe(`${ActionTypes.INITIALIZE} with slack globally enabled and personal notifications disabled`, () => {
        it('should set state', () => {
            const newState = getInitializedState({
                slackSinkEnabled: true,
                emailSinkEnabled: true,
                teamsSinkEnabled: true,
                subscription: undefined,
                slackSettingsChannel: undefined,
                slackSubscriptionChannel: undefined,
                emailSettingsChannel: undefined,
                emailSubscriptionChannel: undefined,
                teamsSettingsChannel: undefined,
                teamsSubscriptionChannel: undefined,
                settingsSinkTypes: [],
            });

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: [
                        EntityChangeType.Deprecated,
                        EntityChangeType.AssertionFailed,
                        EntityChangeType.AssertionPassed,
                        EntityChangeType.AssertionError,
                        EntityChangeType.IncidentRaised,
                        EntityChangeType.IncidentResolved,
                        EntityChangeType.OperationColumnAdded,
                        EntityChangeType.OperationColumnRemoved,
                        EntityChangeType.OperationColumnModified,
                        EntityChangeType.OwnerAdded,
                        EntityChangeType.OwnerRemoved,
                        EntityChangeType.GlossaryTermAdded,
                        EntityChangeType.GlossaryTermRemoved,
                        EntityChangeType.GlossaryTermProposed,
                        EntityChangeType.TagAdded,
                        EntityChangeType.TagRemoved,
                        EntityChangeType.TagProposed,
                    ],
                    expandedKeys: [],
                    keysWithAllFilteringCleared: [],
                },
                settings: {
                    sinkTypes: [],
                    slack: {
                        channel: undefined,
                    },
                    email: {
                        channel: undefined,
                    },
                    teams: {
                        channel: undefined,
                        channelName: undefined,
                    },
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [
                    NotificationSinkType.Slack,
                    NotificationSinkType.Email,
                    NotificationSinkType.Teams,
                ],
                slack: {
                    enabled: true,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: true,
                    },
                },
                email: {
                    enabled: true,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: true,
                    },
                },
                teams: {
                    enabled: true,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: true,
                    },
                    selectedResult: null,
                },
            });
        });
    });

    describe(`${ActionTypes.INITIALIZE} with sinks globally enabled and personal notifications enabled`, () => {
        it('should set state without an existing subscription', () => {
            const newState = getInitializedState({
                slackSinkEnabled: true,
                emailSinkEnabled: true,
                teamsSinkEnabled: true,
                subscription: undefined,
                slackSettingsChannel: 'abc',
                slackSubscriptionChannel: undefined,
                emailSettingsChannel: 'john@test.com',
                emailSubscriptionChannel: undefined,
                teamsSettingsChannel: undefined,
                teamsSubscriptionChannel: undefined,
                settingsSinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
            });

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: [
                        EntityChangeType.Deprecated,
                        EntityChangeType.AssertionFailed,
                        EntityChangeType.AssertionPassed,
                        EntityChangeType.AssertionError,
                        EntityChangeType.IncidentRaised,
                        EntityChangeType.IncidentResolved,
                        EntityChangeType.OperationColumnAdded,
                        EntityChangeType.OperationColumnRemoved,
                        EntityChangeType.OperationColumnModified,
                        EntityChangeType.OwnerAdded,
                        EntityChangeType.OwnerRemoved,
                        EntityChangeType.GlossaryTermAdded,
                        EntityChangeType.GlossaryTermRemoved,
                        EntityChangeType.GlossaryTermProposed,
                        EntityChangeType.TagAdded,
                        EntityChangeType.TagRemoved,
                        EntityChangeType.TagProposed,
                    ],
                    expandedKeys: [],
                    keysWithAllFilteringCleared: [],
                },
                settings: {
                    sinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
                    slack: {
                        channel: 'abc',
                    },
                    email: {
                        channel: 'john@test.com',
                    },
                    teams: {
                        channel: undefined,
                        channelName: undefined,
                    },
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [
                    NotificationSinkType.Slack,
                    NotificationSinkType.Email,
                    NotificationSinkType.Teams,
                ],
                slack: {
                    enabled: true,
                    channelSelection: ChannelSelections.SETTINGS,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                },
                email: {
                    enabled: true,
                    channelSelection: ChannelSelections.SETTINGS,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                },
                teams: {
                    enabled: true,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: true,
                    },
                    selectedResult: null,
                },
            });
        });

        it('should set state with an existing subscription', () => {
            const newState = getInitializedState({
                slackSinkEnabled: true,
                emailSinkEnabled: true,
                teamsSinkEnabled: true,
                subscription: slackSubscription,
                slackSettingsChannel: 'abc',
                slackSubscriptionChannel: 'xyz',
                emailSettingsChannel: 'base@test.com',
                emailSubscriptionChannel: 'john@test.com',
                teamsSettingsChannel: undefined,
                teamsSubscriptionChannel: undefined,
                settingsSinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
            });

            expect(newState).toEqual({
                edited: false,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: [],
                    expandedKeys: [],
                    keysWithAllFilteringCleared: [],
                },
                settings: {
                    sinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
                    slack: {
                        channel: 'abc',
                    },
                    email: {
                        channel: 'base@test.com',
                    },
                    teams: {
                        channel: undefined,
                        channelName: undefined,
                    },
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
                slack: {
                    enabled: true,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: 'xyz',
                        saveAsDefault: false,
                    },
                },
                email: {
                    enabled: true,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: 'john@test.com',
                        saveAsDefault: false,
                    },
                },
                teams: {
                    enabled: false,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                    selectedResult: null,
                },
            });
        });
    });

    describe(`${ActionTypes.SET_SLACK_ENABLED}`, () => {
        it('should enable slack state', () => {
            const state = getInitializedState({
                slackSinkEnabled: false,
                emailSinkEnabled: false,
                teamsSinkEnabled: false,
                subscription: slackSubscription,
                slackSettingsChannel: 'abc',
                slackSubscriptionChannel: undefined,
                emailSettingsChannel: 'test@test.com',
                emailSubscriptionChannel: undefined,
                teamsSettingsChannel: undefined,
                teamsSubscriptionChannel: undefined,
                settingsSinkTypes: [],
            });

            const action = {
                type: ActionTypes.SET_SLACK_ENABLED,
                payload: true,
            };

            const newState = reducer(state, action);

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: [],
                    expandedKeys: [],
                    keysWithAllFilteringCleared: [],
                },
                settings: {
                    sinkTypes: [],
                    slack: {
                        channel: 'abc',
                    },
                    email: {
                        channel: 'test@test.com',
                    },
                    teams: {
                        channel: undefined,
                        channelName: undefined,
                    },
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Email, NotificationSinkType.Slack],
                slack: {
                    enabled: true,
                    channelSelection: ChannelSelections.SETTINGS,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                },
                email: {
                    enabled: false,
                    channelSelection: ChannelSelections.SETTINGS,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                },
                teams: {
                    enabled: false,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                    selectedResult: null,
                },
            });
        });
    });

    describe(`${ActionTypes.SET_EMAIL_ENABLED}`, () => {
        it('should enable email state', () => {
            const state = getInitializedState({
                slackSinkEnabled: false,
                emailSinkEnabled: false,
                teamsSinkEnabled: false,
                subscription: slackSubscription,
                slackSettingsChannel: 'abc',
                slackSubscriptionChannel: undefined,
                emailSettingsChannel: 'test@test.com',
                emailSubscriptionChannel: undefined,
                teamsSettingsChannel: undefined,
                teamsSubscriptionChannel: undefined,
                settingsSinkTypes: [],
            });

            const action = {
                type: ActionTypes.SET_EMAIL_ENABLED,
                payload: true,
            };

            const newState = reducer(state, action);

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: [],
                    expandedKeys: [],
                    keysWithAllFilteringCleared: [],
                },
                settings: {
                    sinkTypes: [],
                    slack: {
                        channel: 'abc',
                    },
                    email: {
                        channel: 'test@test.com',
                    },
                    teams: {
                        channel: undefined,
                        channelName: undefined,
                    },
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Email, NotificationSinkType.Slack],
                slack: {
                    enabled: false,
                    channelSelection: ChannelSelections.SETTINGS,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                },
                email: {
                    enabled: true,
                    channelSelection: ChannelSelections.SETTINGS,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                },
                teams: {
                    enabled: false,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                    selectedResult: null,
                },
            });
        });
    });

    describe(`${ActionTypes.SET_SLACK_CHANNEL_SELECTION}`, () => {
        it('should set selection to settings', () => {
            const state = getInitializedState({
                slackSinkEnabled: true,
                teamsSinkEnabled: true,
                emailSinkEnabled: true,
                subscription: slackSubscription,
                slackSettingsChannel: 'abc',
                slackSubscriptionChannel: 'xyz',
                emailSettingsChannel: 'base@test.com',
                emailSubscriptionChannel: 'test@test.com',
                teamsSettingsChannel: undefined,
                teamsSubscriptionChannel: undefined,
                settingsSinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
            });

            const action = {
                type: ActionTypes.SET_SLACK_CHANNEL_SELECTION,
                payload: ChannelSelections.SETTINGS,
            };

            const newState = reducer(state, action);

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: [],
                    expandedKeys: [],
                    keysWithAllFilteringCleared: [],
                },
                settings: {
                    sinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
                    slack: {
                        channel: 'abc',
                    },
                    email: {
                        channel: 'base@test.com',
                    },
                    teams: {
                        channel: undefined,
                        channelName: undefined,
                    },
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
                slack: {
                    enabled: true,
                    channelSelection: ChannelSelections.SETTINGS,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                },
                email: {
                    enabled: true,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: 'test@test.com',
                        saveAsDefault: false,
                    },
                },
                teams: {
                    enabled: false,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                    selectedResult: null,
                },
            });

            const action2 = {
                type: ActionTypes.SET_EMAIL_CHANNEL_SELECTION,
                payload: ChannelSelections.SETTINGS,
            };

            const newState2 = reducer(state, action2);

            expect(newState2).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: [],
                    expandedKeys: [],
                    keysWithAllFilteringCleared: [],
                },
                settings: {
                    sinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
                    slack: {
                        channel: 'abc',
                    },
                    email: {
                        channel: 'base@test.com',
                    },
                    teams: {
                        channel: undefined,
                        channelName: undefined,
                    },
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
                slack: {
                    enabled: true,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: 'xyz',
                        saveAsDefault: false,
                    },
                },
                email: {
                    enabled: true,
                    channelSelection: ChannelSelections.SETTINGS,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                },
                teams: {
                    enabled: false,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                    selectedResult: null,
                },
            });
        });

        it('should set selection to subscription', () => {
            const state = getInitializedState({
                slackSinkEnabled: true,
                teamsSinkEnabled: true,
                emailSinkEnabled: true,
                subscription: slackSubscription,
                slackSettingsChannel: 'abc',
                slackSubscriptionChannel: 'xyz',
                emailSettingsChannel: 'base@test.com',
                emailSubscriptionChannel: 'test@test.com',
                settingsSinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
            });

            const action = {
                type: ActionTypes.SET_SLACK_CHANNEL_SELECTION,
                payload: ChannelSelections.SUBSCRIPTION,
            };

            const newState = reducer(state, action);

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: [],
                    expandedKeys: [],
                    keysWithAllFilteringCleared: [],
                },
                settings: {
                    sinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
                    slack: {
                        channel: 'abc',
                    },
                    email: {
                        channel: 'base@test.com',
                    },
                    teams: {
                        channel: undefined,
                        channelName: undefined,
                    },
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
                slack: {
                    enabled: true,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: 'xyz',
                        saveAsDefault: false,
                    },
                },
                email: {
                    enabled: true,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: 'test@test.com',
                        saveAsDefault: false,
                    },
                },
                teams: {
                    enabled: false,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                    selectedResult: null,
                },
            });

            const action2 = {
                type: ActionTypes.SET_EMAIL_CHANNEL_SELECTION,
                payload: ChannelSelections.SUBSCRIPTION,
            };

            const newState2 = reducer(state, action2);

            expect(newState2).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: [],
                    expandedKeys: [],
                    keysWithAllFilteringCleared: [],
                },
                settings: {
                    sinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
                    slack: {
                        channel: 'abc',
                    },
                    email: {
                        channel: 'base@test.com',
                    },
                    teams: {
                        channel: undefined,
                        channelName: undefined,
                    },
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
                slack: {
                    enabled: true,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: 'xyz',
                        saveAsDefault: false,
                    },
                },
                email: {
                    enabled: true,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: 'test@test.com',
                        saveAsDefault: false,
                    },
                },
                teams: {
                    enabled: false,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                    selectedResult: null,
                },
            });
        });
    });

    describe(`${ActionTypes.SET_TEAMS_ENABLED}`, () => {
        it('should enable teams state', () => {
            const state = getInitializedState({
                slackSinkEnabled: false,
                emailSinkEnabled: false,
                teamsSinkEnabled: false,
                subscription: slackSubscription,
                slackSettingsChannel: 'abc',
                slackSubscriptionChannel: undefined,
                emailSettingsChannel: 'test@test.com',
                emailSubscriptionChannel: undefined,
                settingsSinkTypes: [],
            });

            const action = {
                type: ActionTypes.SET_TEAMS_ENABLED,
                payload: true,
            };

            const newState = reducer(state, action);

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: [],
                    expandedKeys: [],
                    keysWithAllFilteringCleared: [],
                },
                settings: {
                    sinkTypes: [],
                    slack: {
                        channel: 'abc',
                    },
                    email: {
                        channel: 'test@test.com',
                    },
                    teams: {
                        channel: undefined,
                        channelName: undefined,
                    },
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [
                    NotificationSinkType.Slack,
                    NotificationSinkType.Email,
                    NotificationSinkType.Teams,
                ],
                slack: {
                    enabled: false,
                    channelSelection: ChannelSelections.SETTINGS,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                },
                email: {
                    enabled: false,
                    channelSelection: ChannelSelections.SETTINGS,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                },
                teams: {
                    enabled: true,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                    selectedResult: null,
                },
            });
        });
    });

    describe(`${ActionTypes.SET_TEAMS_CHANNEL_SELECTION}`, () => {
        it('should set teams channel selection to settings', () => {
            const state = getInitializedState({
                slackSinkEnabled: true,
                emailSinkEnabled: true,
                teamsSinkEnabled: true,
                subscription: slackSubscription,
                slackSettingsChannel: 'abc',
                slackSubscriptionChannel: 'xyz',
                emailSettingsChannel: 'base@test.com',
                emailSubscriptionChannel: 'test@test.com',
                settingsSinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
            });

            const action = {
                type: ActionTypes.SET_TEAMS_CHANNEL_SELECTION,
                payload: ChannelSelections.SETTINGS,
            };

            const newState = reducer(state, action);

            expect(newState.teams.channelSelection).toBe(ChannelSelections.SETTINGS);
            expect(newState.edited).toBe(true);
        });

        it('should set teams channel selection to subscription', () => {
            const state = getInitializedState({
                slackSinkEnabled: true,
                emailSinkEnabled: true,
                teamsSinkEnabled: true,
                subscription: slackSubscription,
                slackSettingsChannel: 'abc',
                slackSubscriptionChannel: 'xyz',
                emailSettingsChannel: 'base@test.com',
                emailSubscriptionChannel: 'test@test.com',
                settingsSinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
            });

            const action = {
                type: ActionTypes.SET_TEAMS_CHANNEL_SELECTION,
                payload: ChannelSelections.SUBSCRIPTION,
            };

            const newState = reducer(state, action);

            expect(newState.teams.channelSelection).toBe(ChannelSelections.SUBSCRIPTION);
            expect(newState.edited).toBe(true);
        });
    });

    describe(`${ActionTypes.SET_TEAMS_SUBSCRIPTION_CHANNEL}`, () => {
        it('should set teams subscription channel', () => {
            const state = getInitializedState({
                slackSinkEnabled: true,
                emailSinkEnabled: true,
                teamsSinkEnabled: true,
                subscription: slackSubscription,
                slackSettingsChannel: 'abc',
                slackSubscriptionChannel: 'xyz',
                emailSettingsChannel: 'base@test.com',
                emailSubscriptionChannel: 'test@test.com',
                settingsSinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
            });

            const action = {
                type: ActionTypes.SET_TEAMS_SUBSCRIPTION_CHANNEL,
                payload: 'teams-channel-123',
            };

            const newState = reducer(state, action);

            expect(newState.teams.subscription.channel).toBe('teams-channel-123');
            expect(newState.edited).toBe(true);
        });
    });

    describe(`${ActionTypes.SET_TEAMS_SAVE_AS_DEFAULT}`, () => {
        it('should set teams save as default', () => {
            const state = getInitializedState({
                slackSinkEnabled: true,
                emailSinkEnabled: true,
                teamsSinkEnabled: true,
                subscription: slackSubscription,
                slackSettingsChannel: 'abc',
                slackSubscriptionChannel: 'xyz',
                emailSettingsChannel: 'base@test.com',
                emailSubscriptionChannel: 'test@test.com',
                settingsSinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
            });

            const action = {
                type: ActionTypes.SET_TEAMS_SAVE_AS_DEFAULT,
                payload: false,
            };

            const newState = reducer(state, action);

            expect(newState.teams.subscription.saveAsDefault).toBe(false);
            expect(newState.edited).toBe(true);
        });
    });

    describe(`${ActionTypes.SET_TEAMS_OBJECT}`, () => {
        it('should set whole teams object', () => {
            const state = getInitializedState({
                slackSinkEnabled: true,
                emailSinkEnabled: true,
                teamsSinkEnabled: true,
                subscription: slackSubscription,
                slackSettingsChannel: 'abc',
                slackSubscriptionChannel: 'xyz',
                emailSettingsChannel: 'base@test.com',
                emailSubscriptionChannel: 'test@test.com',
                settingsSinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
            });

            const newTeamsState = {
                enabled: false,
                channelSelection: ChannelSelections.SETTINGS,
                subscription: {
                    channel: 'new-teams-channel',
                    saveAsDefault: false,
                },
                selectedResult: {
                    id: 'team-123',
                    displayName: 'Engineering Team',
                    type: 'channel' as const,
                    teamName: 'Engineering Team',
                },
            };

            const action = {
                type: ActionTypes.SET_TEAMS_OBJECT,
                payload: newTeamsState,
            };

            const newState = reducer(state, action);

            expect(newState.teams).toEqual(newTeamsState);
            expect(newState.edited).toBe(true);
        });
    });

    describe('Teams subscription saveAsDefault logic bug', () => {
        it('should set saveAsDefault to false when there is an existing subscription with Teams settings', () => {
            const teamsSubscription: Partial<DataHubSubscription> = {
                entityChangeTypes: [],
                notificationConfig: {
                    notificationSettings: {
                        sinkTypes: [NotificationSinkType.Teams],
                        teamsSettings: {
                            channels: [{ id: 'teams-demo-channel', name: 'Demo Channel' }],
                        },
                    },
                },
            };

            const newState = getInitializedState({
                slackSinkEnabled: false,
                emailSinkEnabled: false,
                teamsSinkEnabled: true,
                subscription: teamsSubscription,
                slackSettingsChannel: undefined,
                slackSubscriptionChannel: undefined,
                emailSettingsChannel: undefined,
                emailSubscriptionChannel: undefined,
                teamsSettingsChannel: undefined,
                teamsSubscriptionChannel: 'teams-demo-channel',
                settingsSinkTypes: [],
            });

            // When there's an existing subscription, saveAsDefault should be false
            // so that the selected channel gets saved to the subscription
            expect(newState.teams.subscription.saveAsDefault).toBe(false);
            expect(newState.teams.subscription.channel).toBe('teams-demo-channel');
            expect(newState.teams.enabled).toBe(true);
        });

        it('should set saveAsDefault to true when there is no existing subscription and no settings channel', () => {
            const newState = getInitializedState({
                slackSinkEnabled: false,
                emailSinkEnabled: false,
                teamsSinkEnabled: true,
                subscription: undefined,
                slackSettingsChannel: undefined,
                slackSubscriptionChannel: undefined,
                emailSettingsChannel: undefined,
                emailSubscriptionChannel: undefined,
                teamsSettingsChannel: undefined,
                teamsSubscriptionChannel: undefined,
                settingsSinkTypes: [],
            });

            // When there's no existing subscription and no settings channel,
            // saveAsDefault should be true (use default settings)
            expect(newState.teams.subscription.saveAsDefault).toBe(true);
            expect(newState.teams.subscription.channel).toBe(undefined);
            expect(newState.teams.enabled).toBe(true);
        });
    });

    describe('Teams settings corruption prevention', () => {
        it('should store Teams channel settings with both ID and name in state', () => {
            const newState = getInitializedState({
                slackSinkEnabled: false,
                emailSinkEnabled: false,
                teamsSinkEnabled: true,
                subscription: undefined,
                slackSettingsChannel: undefined,
                slackSubscriptionChannel: undefined,
                emailSettingsChannel: undefined,
                emailSubscriptionChannel: undefined,
                teamsSettingsChannel: '19:teams-demo-channel@thread.teams',
                teamsSettingsChannelName: 'Demo Channel',
                teamsSubscriptionChannel: undefined,
                settingsSinkTypes: [NotificationSinkType.Teams],
            });

            // Teams settings should be stored with both ID and display name
            expect(newState.settings.teams.channel).toBe('19:teams-demo-channel@thread.teams');
            expect(newState.settings.teams.channelName).toBe('Demo Channel');
            expect(newState.teams.channelSelection).toBe('SETTINGS');
        });

        it('should handle Teams channel settings when only ID is provided', () => {
            const newState = getInitializedState({
                slackSinkEnabled: false,
                emailSinkEnabled: false,
                teamsSinkEnabled: true,
                subscription: undefined,
                slackSettingsChannel: undefined,
                slackSubscriptionChannel: undefined,
                emailSettingsChannel: undefined,
                emailSubscriptionChannel: undefined,
                teamsSettingsChannel: '19:teams-demo-channel@thread.teams',
                teamsSettingsChannelName: undefined,
                teamsSubscriptionChannel: undefined,
                settingsSinkTypes: [NotificationSinkType.Teams],
            });

            // Teams settings should still work with just ID (name can be undefined)
            expect(newState.settings.teams.channel).toBe('19:teams-demo-channel@thread.teams');
            expect(newState.settings.teams.channelName).toBe(undefined);
            expect(newState.teams.channelSelection).toBe('SETTINGS');
        });
    });
});
