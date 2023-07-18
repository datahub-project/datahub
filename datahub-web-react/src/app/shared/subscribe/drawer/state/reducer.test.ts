import { DataHubSubscription, EntityType, NotificationSinkType } from '../../../../../types.generated';
import { getDefaultCheckedKeys } from '../utils';
import { createInitialState, reducer } from './reducer';
import { ActionTypes, ChannelSelection, SettingsSelection, SubscriptionSelection } from './types';

const entityType = EntityType.Dataset;
const slackSubscription: Partial<DataHubSubscription> = {
    notificationConfig: {
        sinkTypes: [NotificationSinkType.Slack],
    },
};

const getInitializedState = ({
    subscription,
    settingsChannel,
    subscriptionChannel,
    slackSinkEnabled,
}: {
    subscription?: Partial<DataHubSubscription>;
    settingsChannel?: string;
    subscriptionChannel?: string;
    slackSinkEnabled: boolean;
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
        },
    };

    return reducer(state, action);
};

describe('reducer', () => {
    describe('initialize with slack globally disabled', () => {
        it('set initial state', () => {
            const newState = getInitializedState({ slackSinkEnabled: false, subscription: undefined });

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: getDefaultCheckedKeys(entityType),
                    expandedKeys: [],
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [],
                slack: {
                    enabled: false,
                    channelSelection: 'subscription',
                    settings: {
                        channel: undefined,
                    },
                    subscription: {
                        channel: undefined,
                        saveAsDefault: true,
                    },
                },
            });
        });
    });

    describe('initialize with slack globally enabled and personal notifications disabled', () => {
        it('set initial state', () => {
            const newState = getInitializedState({ slackSinkEnabled: true, subscription: undefined });

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: getDefaultCheckedKeys(entityType),
                    expandedKeys: [],
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [],
                slack: {
                    enabled: false,
                    channelSelection: 'subscription',
                    settings: {
                        channel: undefined,
                    },
                    subscription: {
                        channel: undefined,
                        saveAsDefault: true,
                    },
                },
            });
        });
    });

    describe('initialize with slack globally enabled and personal notifications enabled', () => {
        it('should set initial state without an existing subscription', () => {
            const newState = getInitializedState({
                slackSinkEnabled: true,
                settingsChannel: 'abc',
                subscription: undefined,
            });

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: getDefaultCheckedKeys(entityType),
                    expandedKeys: [],
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [],
                slack: {
                    enabled: false,
                    channelSelection: 'settings',
                    settings: {
                        channel: 'abc',
                    },
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                },
            });
        });

        it('should set initial state with an existing subscription', () => {
            const newState = getInitializedState({
                slackSinkEnabled: true,
                settingsChannel: 'abc',
                subscriptionChannel: 'xyz',
                subscription: slackSubscription,
            });

            expect(newState).toEqual({
                edited: false,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: getDefaultCheckedKeys(entityType),
                    expandedKeys: [],
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Slack],
                slack: {
                    enabled: true,
                    channelSelection: 'subscription',
                    settings: {
                        channel: 'abc',
                    },
                    subscription: {
                        channel: 'xyz',
                        saveAsDefault: false,
                    },
                },
            });
        });
    });

    describe('set slack enabled', () => {
        it('should enable slack state', () => {
            const state = getInitializedState({
                slackSinkEnabled: true,
                settingsChannel: 'abc',
                subscriptionChannel: undefined,
                subscription: slackSubscription,
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
                    checkedKeys: getDefaultCheckedKeys(entityType),
                    expandedKeys: [],
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Slack],
                slack: {
                    enabled: true,
                    channelSelection: SettingsSelection,
                    settings: {
                        channel: 'abc',
                    },
                    subscription: {
                        channel: undefined,
                        saveAsDefault: false,
                    },
                },
            });
        });
    });

    describe('set channel selection', () => {
        it('should set selection to settings', () => {
            const action = {
                type: ActionTypes.SET_CHANNEL_SELECTION,
                payload: SettingsSelection as ChannelSelection,
            };

            const state = getInitializedState({
                slackSinkEnabled: true,
                settingsChannel: 'abc',
                subscriptionChannel: 'xyz',
                subscription: slackSubscription,
            });

            const newState = reducer(state, action);

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: getDefaultCheckedKeys(entityType),
                    expandedKeys: [],
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Slack],
                slack: {
                    enabled: true,
                    channelSelection: SettingsSelection,
                    settings: {
                        channel: 'abc',
                    },
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
                payload: SubscriptionSelection as ChannelSelection,
            };

            const state = getInitializedState({
                slackSinkEnabled: true,
                settingsChannel: 'abc',
                subscriptionChannel: 'xyz',
                subscription: slackSubscription,
            });
            const newState = reducer(state, action);

            expect(newState).toEqual({
                edited: true,
                isPersonal: true,
                notificationTypes: {
                    checkedKeys: getDefaultCheckedKeys(entityType),
                    expandedKeys: [],
                },
                subscribeToUpstream: false,
                notificationSinkTypes: [NotificationSinkType.Slack],
                slack: {
                    enabled: true,
                    channelSelection: SubscriptionSelection,
                    settings: {
                        channel: 'abc',
                    },
                    subscription: {
                        channel: 'xyz',
                        saveAsDefault: false,
                    },
                },
            });
        });
    });
});
