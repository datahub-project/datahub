import { useMemo } from 'react';
import { useFormDispatchContext } from './context';
import { InitializeActionPayload, State } from './types';

const useFormActions = () => {
    const dispatch = useFormDispatchContext();

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
        }),
        [dispatch],
    );
};

export default useFormActions;
