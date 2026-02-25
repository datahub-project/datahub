/**
 * Semantic color matching utilities for DataHub entity types
 *
 * Provides intelligent matching of series keys to entity colors,
 * handling various naming conventions and formats
 */
import { DATAHUB_ENTITY_COLORS } from '@app/analyticsDashboardV2/utils/chartColorConstants';

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
export function findDataHubEntityColor(seriesKey: string): string | null {
    const normalized = normalizeKey(seriesKey);

    // Direct match
    if (DATAHUB_ENTITY_COLORS[normalized]) {
        return DATAHUB_ENTITY_COLORS[normalized];
    }

    // Try removing common suffixes
    const withoutSuffix = normalized.replace(/(type|entity|tab|view)s?$/i, '');
    if (DATAHUB_ENTITY_COLORS[withoutSuffix]) {
        return DATAHUB_ENTITY_COLORS[withoutSuffix];
    }

    // Try removing "data" prefix (for matching "dataproduct" -> "product")
    const withoutDataPrefix = normalized.replace(/^data/, '');
    if (DATAHUB_ENTITY_COLORS[withoutDataPrefix]) {
        return DATAHUB_ENTITY_COLORS[withoutDataPrefix];
    }

    // Partial matching for compound names
    const matchEntry = Object.entries(DATAHUB_ENTITY_COLORS).find(
        ([entityType]) => normalized.includes(entityType) || entityType.includes(normalized),
    );

    return matchEntry ? matchEntry[1] : null;
}

/**
 * Check if a series key likely represents a DataHub entity
 * Useful for debugging and testing
 */
export function isDataHubEntity(seriesKey: string): boolean {
    return findDataHubEntityColor(seriesKey) !== null;
}

/**
 * Get all possible entity matches for a series key
 * Useful for debugging ambiguous matches
 */
export function getAllEntityMatches(seriesKey: string): Array<{ entity: string; color: string }> {
    const normalized = normalizeKey(seriesKey);

    return Object.entries(DATAHUB_ENTITY_COLORS)
        .filter(
            ([entityType]) =>
                normalized === entityType || normalized.includes(entityType) || entityType.includes(normalized),
        )
        .map(([entity, color]) => ({ entity, color }));
}
