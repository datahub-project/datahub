import { useState, useCallback } from 'react';
import { useHistory, useLocation } from 'react-router-dom';

const IS_VISUALIZE_VIEW_DEFAULT = true;

export function useLineageViewState() {
    const history = useHistory();
    const location = useLocation();

    // Read query parameter to determine the initial state
    const initialView = new URLSearchParams(location.search).get('lineageView');
    const [isVisualizeView, setIsVisualizeView] = useState(
        initialView === 'impact' ? false : IS_VISUALIZE_VIEW_DEFAULT,
    );

    const setVisualizeView = useCallback(
        (view: boolean) => {
            setIsVisualizeView(view);

            // Update the URL with the new view state
            const searchParams = new URLSearchParams(location.search);
            searchParams.set('lineageView', view ? 'explorer' : 'impact');
            history.replace({ search: searchParams.toString() });
        },
        [location.search, history],
    );

    return {
        isVisualizeView,
        setVisualizeView,
    };
}
