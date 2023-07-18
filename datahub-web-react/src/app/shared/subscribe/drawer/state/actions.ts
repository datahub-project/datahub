import { Key, useMemo } from 'react';
import { useDrawerDispatch } from './context';
import { ActionTypes, ChannelSelection, InitializeActionPayload } from './types';

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
            setChannelSelection: (payload: ChannelSelection) => {
                dispatch({ type: ActionTypes.SET_CHANNEL_SELECTION, payload });
            },
            setSubscriptionChannel: (payload: string) => {
                dispatch({ type: ActionTypes.SET_SUBSCRIPTION_CHANNEL, payload });
            },
            setSaveAsDefault: (payload: boolean) => {
                dispatch({ type: ActionTypes.SET_SAVE_AS_DEFAULT, payload });
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
        }),
        [dispatch],
    );
};

export default useDrawerActions;
