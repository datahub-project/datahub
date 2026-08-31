import { DefaultTheme } from 'styled-components';

import { getDatahubEntityColors } from '@app/analyticsDashboardV2/utils/chartColorConstants';

/**
 * Semantic color matching utilities for DataHub entity types
 *
 * Provides intelligent matching of series keys to entity colors,
 * handling various naming conventions and formats
 */
const getEntityColorsObject = (theme: DefaultTheme) => {
    return getDatahubEntityColors(theme);
};
/**
 * Normalize a series key for matching
 * Removes separators, converts to lowercase
 */
function normalizeKey(key: string): string {
    return key.toLowerCase().replace(/[_-\s]/g, ''); // Remove separators
}

/**
 * Find a semantic color for a DataHub entity based on the series key
 *
 * Matching strategy:
 * 1. Direct match after normalization
 * 2. Match by removing common suffixes (type, entity, tab, view)
 * 3. Match by removing "data" prefix
 * 4. Partial/substring matching for compound names
 *
 * @param seriesKey - The series identifier (e.g., "DATASET", "data_product", "Schema View")
 * @returns Hex color string if match found, null otherwise
 */
export function findDataHubEntityColor(seriesKey: string, theme: DefaultTheme): string | null {
    const datahubEntityColors = getEntityColorsObject(theme);
    const normalized = normalizeKey(seriesKey);

    // Direct match
    if (datahubEntityColors[normalized]) {
        return datahubEntityColors[normalized];
    }

    // Try removing common suffixes
    const withoutSuffix = normalized.replace(/(type|entity|tab|view)s?$/i, '');
    if (datahubEntityColors[withoutSuffix]) {
        return datahubEntityColors[withoutSuffix];
    }

    // Try removing "data" prefix (for matching "dataproduct" -> "product")
    const withoutDataPrefix = normalized.replace(/^data/, '');
    if (datahubEntityColors[withoutDataPrefix]) {
        return datahubEntityColors[withoutDataPrefix];
    }

    // Partial matching for compound names
    const matchEntry = Object.entries(datahubEntityColors).find(
        ([entityType]) => normalized.includes(entityType) || entityType.includes(normalized),
    );

    return matchEntry ? matchEntry[1] : null;
}

/**
 * Get all possible entity matches for a series key
 * Useful for debugging ambiguous matches
 */
export function getAllEntityMatches(seriesKey: string, theme: DefaultTheme): Array<{ entity: string; color: string }> {
    const normalized = normalizeKey(seriesKey);
    const datahubEntityColors = getEntityColorsObject(theme);

    return Object.entries(datahubEntityColors)
        .filter(
            ([entityType]) =>
                normalized === entityType || normalized.includes(entityType) || entityType.includes(normalized),
        )
        .map(([entity, color]) => ({ entity, color }));
}
