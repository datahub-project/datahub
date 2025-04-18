import { Key, useMemo } from 'react';

import { useDrawerDispatch } from '@app/shared/subscribe/drawer/state/context';
import {
    ActionTypes,
    ChannelSelection,
    InitializeActionPayload,
    SlackState,
} from '@app/shared/subscribe/drawer/state/types';

const useDrawerActions = () => {
    const dispatch = useDrawerDispatch();

    return useMemo(
        () => ({
            initialize: (payload: InitializeActionPayload) => {
                dispatch({
                    type: ActionTypes.INITIALIZE,
                    payload,
                });
            },
            setSlackEnabled: (payload: boolean) => {
                dispatch({ type: ActionTypes.SET_SLACK_ENABLED, payload });
            },
            setSlackChannelSelection: (payload: ChannelSelection) => {
                dispatch({ type: ActionTypes.SET_SLACK_CHANNEL_SELECTION, payload });
            },
            setSlackSubscriptionChannel: (payload: string) => {
                dispatch({ type: ActionTypes.SET_SLACK_SUBSCRIPTION_CHANNEL, payload });
            },
            setSlackSaveAsDefault: (payload: boolean) => {
                dispatch({ type: ActionTypes.SET_SLACK_SAVE_AS_DEFAULT, payload });
            },
            setWholeSlackObject: (payload: SlackState) => {
                dispatch({ type: ActionTypes.SET_SLACK_OBJECT, payload });
            },
            setEmailEnabled: (payload: boolean) => {
                dispatch({ type: ActionTypes.SET_EMAIL_ENABLED, payload });
            },
            setEmailChannelSelection: (payload: ChannelSelection) => {
                dispatch({ type: ActionTypes.SET_EMAIL_CHANNEL_SELECTION, payload });
            },
            setEmailSubscriptionChannel: (payload: string) => {
                dispatch({ type: ActionTypes.SET_EMAIL_SUBSCRIPTION_CHANNEL, payload });
            },
            setEmailSaveAsDefault: (payload: boolean) => {
                dispatch({ type: ActionTypes.SET_EMAIL_SAVE_AS_DEFAULT, payload });
            },
            setWholeEmailObject: (payload: SlackState) => {
                dispatch({ type: ActionTypes.SET_EMAIL_OBJECT, payload });
            },
            setSubscribeToUpstream: (payload: boolean) => {
                dispatch({ type: ActionTypes.SET_SUBSCRIBE_TO_UPSTREAM, payload });
            },
            setNotificationTypes: (payload: Array<Key>) => {
                dispatch({ type: ActionTypes.SET_NOTIFICATION_TYPES, payload });
            },
            setExpandedNotificationTypes: (payload: Array<Key>) => {
                dispatch({ type: ActionTypes.SET_EXPANDED_NOTIFICATION_TYPES, payload });
            },
            setNotificationTypesWithFiltersCleared: (payload: Array<Key>) => {
                dispatch({ type: ActionTypes.SET_NOTIFICATION_TYPES_WITH_FILTERS_CLEARED, payload });
            },
        }),
        [dispatch],
    );
};

export default useDrawerActions;
