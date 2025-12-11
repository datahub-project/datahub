/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
