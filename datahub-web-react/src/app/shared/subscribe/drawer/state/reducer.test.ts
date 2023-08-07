import { DataHubSubscription, EntityType, NotificationSinkType } from '../../../../../types.generated';
import { createInitialState, reducer } from './reducer';
import { ActionTypes, ChannelSelections } from './types';

const entityType = EntityType.Dataset;
const slackSubscription: Partial<DataHubSubscription> = {
    entityChangeTypes: [],
    notificationConfig: {
        notificationSettings: {
            sinkTypes: [NotificationSinkType.Slack],
            slackSettings: {},
        },
    },
};

const getInitializedState = ({
    slackSinkEnabled,
    subscription,
    settingsChannel,
    subscriptionChannel,
    settingsSinkTypes,
}: {
    slackSinkEnabled: boolean;
    subscription?: Partial<DataHubSubscription>;
    settingsChannel?: string;
    subscriptionChannel?: string;
    settingsSinkTypes?: NotificationSinkType[];
}) => {
    const state = createInitialState();
    const action = {
        type: ActionTypes.INITIALIZE,
        payload: {
            isPersonal: true,
            slackSinkEnabled,
            entityType,
            subscription: subscription as DataHubSubscription,
            subscriptionChannel,
            settingsChannel,
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
                subscription: undefined,
                settingsChannel: undefined,
                subscriptionChannel: undefined,
                settingsSinkTypes: [],
            });

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: [],
                    expandedKeys: [],
                },
                settings: {
                    sinkTypes: [],
                    slack: {
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
            });
        });
    });

    describe(`${ActionTypes.INITIALIZE} with slack globally enabled and personal notifications disabled`, () => {
        it('should set state', () => {
            const newState = getInitializedState({
                slackSinkEnabled: true,
                subscription: undefined,
                settingsChannel: undefined,
                subscriptionChannel: undefined,
                settingsSinkTypes: [],
            });

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: [],
                    expandedKeys: [],
                },
                settings: {
                    sinkTypes: [],
                    slack: {
                        channel: undefined,
                    },
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Slack],
                slack: {
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

    describe(`${ActionTypes.INITIALIZE} with slack globally enabled and personal notifications enabled`, () => {
        it('should set state without an existing subscription', () => {
            const newState = getInitializedState({
                slackSinkEnabled: true,
                subscription: undefined,
                settingsChannel: 'abc',
                subscriptionChannel: undefined,
                settingsSinkTypes: [NotificationSinkType.Slack],
            });

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: [],
                    expandedKeys: [],
                },
                settings: {
                    sinkTypes: [NotificationSinkType.Slack],
                    slack: {
                        channel: 'abc',
                    },
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Slack],
                slack: {
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
                subscription: slackSubscription,
                settingsChannel: 'abc',
                subscriptionChannel: 'xyz',
                settingsSinkTypes: [NotificationSinkType.Slack],
            });

            expect(newState).toEqual({
                edited: false,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: [],
                    expandedKeys: [],
                },
                settings: {
                    sinkTypes: [NotificationSinkType.Slack],
                    slack: {
                        channel: 'abc',
                    },
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Slack],
                slack: {
                    enabled: true,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: 'xyz',
                        saveAsDefault: false,
                    },
                },
            });
        });
    });

    describe(`${ActionTypes.SET_SLACK_ENABLED}`, () => {
        it('should enable slack state', () => {
            const state = getInitializedState({
                slackSinkEnabled: true,
                subscription: slackSubscription,
                settingsChannel: 'abc',
                subscriptionChannel: undefined,
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
                },
                settings: {
                    sinkTypes: [],
                    slack: {
                        channel: 'abc',
                    },
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Slack],
                slack: {
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

    describe(`${ActionTypes.SET_CHANNEL_SELECTION}`, () => {
        it('should set selection to settings', () => {
            const state = getInitializedState({
                slackSinkEnabled: true,
                settingsChannel: 'abc',
                subscriptionChannel: 'xyz',
                subscription: slackSubscription,
                settingsSinkTypes: [NotificationSinkType.Slack],
            });

            const action = {
                type: ActionTypes.SET_CHANNEL_SELECTION,
                payload: ChannelSelections.SETTINGS,
            };

            const newState = reducer(state, action);

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: [],
                    expandedKeys: [],
                },
                settings: {
                    sinkTypes: [NotificationSinkType.Slack],
                    slack: {
                        channel: 'abc',
                    },
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Slack],
                slack: {
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
            const action = {
                type: ActionTypes.SET_CHANNEL_SELECTION,
                payload: ChannelSelections.SUBSCRIPTION,
            };

            const state = getInitializedState({
                slackSinkEnabled: true,
                settingsChannel: 'abc',
                subscriptionChannel: 'xyz',
                subscription: slackSubscription,
                settingsSinkTypes: [NotificationSinkType.Slack],
            });
            const newState = reducer(state, action);

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: [],
                    expandedKeys: [],
                },
                settings: {
                    sinkTypes: [NotificationSinkType.Slack],
                    slack: {
                        channel: 'abc',
                    },
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Slack],
                slack: {
                    enabled: true,
                    channelSelection: ChannelSelections.SUBSCRIPTION,
                    subscription: {
                        channel: 'xyz',
                        saveAsDefault: false,
                    },
                },
            });
        });
    });
});
