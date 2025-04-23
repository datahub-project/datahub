import { useCallback, useMemo } from 'react';

import { useUserContext } from '@src/app/context/useUserContext';

export default function useSelectedView() {
    const { localState, updateLocalState } = useUserContext();

    const clearSelectedView = useCallback(() => {
        const newState = { ...localState, selectedViewUrn: null };
        if (JSON.stringify(newState) !== JSON.stringify(localState)) {
            updateLocalState(newState);
        }
    }, [localState, updateLocalState]);

    const hasSelectedView = useMemo(() => !!localState.selectedViewUrn, [localState.selectedViewUrn]);

    return { clearSelectedView, hasSelectedView, selectedView: localState.selectedViewUrn };
}
