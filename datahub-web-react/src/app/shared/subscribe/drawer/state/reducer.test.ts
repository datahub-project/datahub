import { DataHubSubscription, EntityType, NotificationSinkType } from '../../../../../types.generated';
import { createInitialState, reducer } from './reducer';
import { ActionTypes, ChannelSelections } from './types';

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
    subscription,
    slackSettingsChannel,
    slackSubscriptionChannel,
    emailSettingsChannel,
    emailSubscriptionChannel,
    settingsSinkTypes,
}: {
    slackSinkEnabled: boolean;
    emailSinkEnabled: boolean;
    subscription?: Partial<DataHubSubscription>;
    slackSettingsChannel?: string;
    slackSubscriptionChannel?: string;
    emailSettingsChannel?: string;
    emailSubscriptionChannel?: string;
    settingsSinkTypes?: NotificationSinkType[];
}) => {
    const state = createInitialState();
    const action = {
        type: ActionTypes.INITIALIZE,
        payload: {
            isPersonal: true,
            slackSinkEnabled,
            emailSinkEnabled,
            entityType,
            subscription: subscription as DataHubSubscription,
            slackSubscriptionChannel,
            slackSettingsChannel,
            emailSettingsChannel,
            emailSubscriptionChannel,
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
                subscription: undefined,
                slackSettingsChannel: undefined,
                slackSubscriptionChannel: undefined,
                emailSettingsChannel: undefined,
                emailSubscriptionChannel: undefined,
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
            });
        });
    });

    describe(`${ActionTypes.INITIALIZE} with slack globally enabled and personal notifications disabled`, () => {
        it('should set state', () => {
            const newState = getInitializedState({
                slackSinkEnabled: true,
                emailSinkEnabled: true,
                subscription: undefined,
                slackSettingsChannel: undefined,
                slackSubscriptionChannel: undefined,
                emailSettingsChannel: undefined,
                emailSubscriptionChannel: undefined,
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
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
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
            });
        });
    });

    describe(`${ActionTypes.INITIALIZE} with sinks globally enabled and personal notifications enabled`, () => {
        it('should set state without an existing subscription', () => {
            const newState = getInitializedState({
                slackSinkEnabled: true,
                emailSinkEnabled: true,
                subscription: undefined,
                slackSettingsChannel: 'abc',
                slackSubscriptionChannel: undefined,
                emailSettingsChannel: 'john@test.com',
                emailSubscriptionChannel: undefined,
                settingsSinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
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
                    sinkTypes: [NotificationSinkType.Slack, NotificationSinkType.Email],
                    slack: {
                        channel: 'abc',
                    },
                    email: {
                        channel: 'john@test.com',
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
                    channelSelection: ChannelSelections.SETTINGS,
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                },
            });
        });

        it('should set state with an existing subscription', () => {
            const newState = getInitializedState({
                slackSinkEnabled: true,
                emailSinkEnabled: true,
                subscription: slackSubscription,
                slackSettingsChannel: 'abc',
                slackSubscriptionChannel: 'xyz',
                emailSettingsChannel: 'base@test.com',
                emailSubscriptionChannel: 'john@test.com',
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
            });
        });
    });

    describe(`${ActionTypes.SET_SLACK_ENABLED}`, () => {
        it('should enable slack state', () => {
            const state = getInitializedState({
                slackSinkEnabled: false,
                emailSinkEnabled: false,
                subscription: slackSubscription,
                slackSettingsChannel: 'abc',
                slackSubscriptionChannel: undefined,
                emailSettingsChannel: 'test@test.com',
                emailSubscriptionChannel: undefined,
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
            });
        });
    });

    describe(`${ActionTypes.SET_EMAIL_ENABLED}`, () => {
        it('should enable email state', () => {
            const state = getInitializedState({
                slackSinkEnabled: false,
                emailSinkEnabled: false,
                subscription: slackSubscription,
                slackSettingsChannel: 'abc',
                slackSubscriptionChannel: undefined,
                emailSettingsChannel: 'test@test.com',
                emailSubscriptionChannel: undefined,
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
            });
        });
    });

    describe(`${ActionTypes.SET_SLACK_CHANNEL_SELECTION}`, () => {
        it('should set selection to settings', () => {
            const state = getInitializedState({
                slackSinkEnabled: true,
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
            });
        });

        it('should set selection to subscription', () => {
            const state = getInitializedState({
                slackSinkEnabled: true,
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
            });
        });
    });
});
