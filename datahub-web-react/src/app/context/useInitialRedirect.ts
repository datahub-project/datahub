import { useEffect } from 'react';
import { useHistory, useLocation } from 'react-router';
import { PageRoutes } from '../../conf/Global';

export function useInitialRedirect(state, localState, setState, setLocalState) {
    const location = useLocation();
    const history = useHistory();

    /**
     * Route to the most recently visited path once on first load of home page, if present in local storage.
     */
    useEffect(() => {
        if (!state.loadedInitialPath) {
            if (location.pathname === PageRoutes.ROOT && localState.selectedPath !== location.pathname) {
                if (localState.selectedPath && !localState.selectedPath.includes(PageRoutes.EMBED)) {
                    history.replace({
                        pathname: localState.selectedPath,
                        search: localState.selectedSearch || '',
                    });
                }
            }
            setState({
                ...state,
                loadedInitialPath: true,
            });
        }
    }, [
        localState.selectedPath,
        localState.selectedSearch,
        location.pathname,
        location.search,
        state,
        history,
        setState,
    ]);

    /**
     * When the location of the browse changes, save the latest to local state.
     */
    useEffect(() => {
        if (
            (localState.selectedPath !== location.pathname || localState.selectedSearch !== location.search) &&
            !location.pathname.includes(PageRoutes.EMBED)
        ) {
            setLocalState({
                ...localState,
                selectedPath: location.pathname,
                selectedSearch: location.search,
            });
        }
    }, [location.pathname, location.search, localState, setLocalState]);
}
