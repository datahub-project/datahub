import { useCallback, useState } from 'react';
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
            const newParam = `lineageView=${view ? 'explorer' : 'impact'}`;

            let newSearch = location.search;
            if (newSearch.includes('lineageView=')) {
                // Replace the existing parameter
                newSearch = newSearch.replace(/(lineageView=)[^&]+/, newParam);
            } else {
                // Add the new parameter
                newSearch += (newSearch ? '&' : '?') + newParam;
            }

            // Update the URL without reloading the page
            history.replace({ search: newSearch });
        },
        [location.search, history],
    );

    return {
        isVisualizeView,
        setVisualizeView,
    };
}
