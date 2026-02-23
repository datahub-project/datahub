import { SelectOption } from '@components/components/Select/types';

import { DataHubView, DataHubViewType } from '@types';

/**
 * Maps a list of DataHub views into SelectOption items for the chat view selector.
 *
 * @param views - Array of DataHubView objects to transform
 * @returns SelectOption[] with view URN as value, view name as label, and type as description
 */
export function buildViewSelectOptions(views: DataHubView[]): SelectOption[] {
    return views.map((view) => ({
        value: view.urn,
        label: view.name,
        description: view.viewType === DataHubViewType.Personal ? 'Personal' : 'Global',
    }));
}

/**
 * Resolves the effective view URN from the selector value.
 *
 * @param selectorValue - The currently selected value from the view dropdown
 * @returns The view URN string, or undefined when no view is selected
 */
export function resolveViewUrn(selectorValue: string | undefined): string | undefined {
    if (!selectorValue) {
        return undefined;
    }
    return selectorValue;
}
