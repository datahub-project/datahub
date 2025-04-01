import { Direction } from '@src/app/lineage/types';
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

    // Helper function to update URL parameters
    const updateUrlParam = useCallback((paramName: string, paramValue: string, currentSearch: string) => {
        if (currentSearch.includes(`${paramName}=`)) {
            // Replace the existing parameter
            return currentSearch.replace(new RegExp(`(${paramName}=)[^&]+`), `${paramName}=${paramValue}`);
        }
        // Add the new parameter
        return `${currentSearch + (currentSearch ? '&' : '?')}${paramName}=${paramValue}`;
    }, []);

    const setVisualizeView = useCallback(
        (view: boolean) => {
            setIsVisualizeView(view);

            // Update the URL with the new view state
            const viewValue = view ? 'explorer' : 'impact';
            const newSearch = updateUrlParam('lineageView', viewValue, location.search);

            // Update the URL without reloading the page
            history.replace({ search: newSearch });
        },
        [location.search, history, updateUrlParam],
    );

    const setVisualizeViewInEditMode = useCallback(
        (view: boolean, direction: Direction) => {
            // First set isVisualizeView state value
            setIsVisualizeView(view);

            // First update lineageView parameter
            const viewValue = view ? 'explorer' : 'impact';
            let newSearch = updateUrlParam('lineageView', viewValue, location.search);

            // Then update lineageEditDirection parameter
            newSearch = updateUrlParam('lineageEditDirection', direction, newSearch);

            // Update URL with both parameters in a single replace
            history.replace({ search: newSearch });
        },
        [location.search, history, updateUrlParam],
    );

    return {
        isVisualizeView,
        setVisualizeView,
        setVisualizeViewInEditMode,
    };
}
