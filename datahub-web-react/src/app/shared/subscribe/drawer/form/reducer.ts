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
        default: {
            return state;
        }
    }
};
