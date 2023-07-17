import { NotificationSinkType, SubscriptionType } from '../../../../../types.generated';
import { ENABLE_UPSTREAM_NOTIFICATIONS } from '../../../../settings/personal/notifications/constants';
import { getDefaultCheckedKeys } from '../utils';
import { Action, State } from './types';

export const initialState: State = {
    // whether the drawer overall is enabled (based only on slack as of now)
    enabled: false,
    checkedKeys: [],
    subscribeToUpstream: false,
    notificationSinkTypes: [],
    slack: {
        // whether slack specifically is enabled
        enabled: false,
        saveAsDefault: false,
    },
};

export const reducer = (state: State, action: Action): State => {
    switch (action.type) {
        case 'initialize': {
            const { slackSinkEnabled, entityType, subscription } = action.payload;
            const entityChangeTypes = subscription?.entityChangeTypes ?? getDefaultCheckedKeys(entityType);
            const sinkTypes = subscription?.notificationConfig?.sinkTypes ?? [];
            const isSlackAndSubscriptionEnabled = slackSinkEnabled && sinkTypes.includes(NotificationSinkType.Slack);
            const hasUpstreamSubscription =
                ENABLE_UPSTREAM_NOTIFICATIONS &&
                !!subscription?.subscriptionTypes?.includes(SubscriptionType.UpstreamEntityChange);

            return {
                ...state,
                checkedKeys: entityChangeTypes,
                subscribeToUpstream: hasUpstreamSubscription,
                slack: {
                    ...state.slack,
                    enabled: isSlackAndSubscriptionEnabled,
                },
            };
        }
        case 'toggleSlack': {
            return {
                ...state,
                slack: {
                    ...state.slack,
                    enabled: action.payload,
                },
            };
        }
        default: {
            return state;
        }
    }
};
