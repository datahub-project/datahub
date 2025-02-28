import { DataHubView } from '@types';

/**
 * Filter views by a search string. Compares name.
 *
 * @param filterText the search text
 * @param views the views to filter
 */
export const filterViews = (filterText, views: DataHubView[]) => {
    const lowerFilterText = filterText.toLowerCase();
    return views.filter((view) => {
        return view.name?.toLowerCase()?.includes(lowerFilterText);
    });
};
