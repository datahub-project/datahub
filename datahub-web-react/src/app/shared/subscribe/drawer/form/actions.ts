import { Key, useMemo } from 'react';
import { useFormDispatch } from './context';
import { InitializeActionPayload, State } from './types';

const useFormActions = () => {
    const dispatch = useFormDispatch();

    return useMemo(
        () => ({
            initialize: (payload: InitializeActionPayload) => {
                dispatch({
                    type: 'initialize',
                    payload,
                });
            },
            setSlackEnabled: (payload: boolean) => {
                dispatch({ type: 'setSlackEnabled', payload });
            },
            setChannelSelection: (payload: State['slack']['channelSelection']) => {
                dispatch({ type: 'setChannelSelection', payload });
            },
            setSubscriptionChannel: (payload: string) => {
                dispatch({ type: 'setSubscriptionChannel', payload });
            },
            setSaveAsDefault: (payload: boolean) => {
                dispatch({ type: 'setSaveAsDefault', payload });
            },
            setCheckedKeys: (payload: Array<Key>) => {
                dispatch({ type: 'setCheckedKeys', payload });
            },
        }),
        [dispatch],
    );
};

export default useFormActions;
