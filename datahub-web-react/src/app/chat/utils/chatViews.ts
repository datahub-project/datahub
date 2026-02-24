import { Icon } from '@components';
import React from 'react';

import { SelectOption } from '@components/components/Select/types';

import { DataHubView, DataHubViewType } from '@types';

/**
 * Returns the appropriate icon for a view's visibility type,
 * matching the icons used in the view type selector (ViewTypeSelectV2).
 *
 * @param viewType - Personal or Global view type
 * @returns A GlobeHemisphereWest icon for global views, Lock icon for personal views
 */
function viewTypeIcon(viewType: DataHubViewType): React.ReactNode {
    const icon = viewType === DataHubViewType.Personal ? 'Lock' : 'GlobeHemisphereWest';
    return React.createElement(Icon, { source: 'phosphor', icon, size: 'md', color: 'gray' });
}

/**
 * Maps a list of DataHub views into SelectOption items for the chat view selector.
 *
 * @param views - Array of DataHubView objects to transform
 * @returns SelectOption[] with view URN as value, view name as label, icon for type, and view description (or type fallback) as description
 */
export function buildViewSelectOptions(views: DataHubView[]): SelectOption[] {
    return views.map((view) => ({
        value: view.urn,
        label: view.name,
        icon: viewTypeIcon(view.viewType),
        description: view.description || undefined,
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
