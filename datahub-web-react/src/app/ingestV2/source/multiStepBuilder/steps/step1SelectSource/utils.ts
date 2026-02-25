import { SourceConfig } from '@app/ingestV2/source/builder/types';

export enum PillLabel {
    New = 'New',
    Popular = 'Popular',
    External = 'External',
}

export const MISCELLANEOUS_CATEGORY_NAME = 'Miscellaneous';
export const DATA_WAREHOUSE_CATEGORY_NAME = 'Data Warehouse';
export const DATA_LAKE_CATEGORY_NAME = 'Data Lake';
export const BI_AND_ANALYTICS_CATEGORY_NAME = 'BI & Analytics';

export const CUSTOM_SOURCE_NAME = 'custom';

const PRESORTED_CATEGORIES_START = [
    DATA_WAREHOUSE_CATEGORY_NAME,
    DATA_LAKE_CATEGORY_NAME,
    BI_AND_ANALYTICS_CATEGORY_NAME,
];
const PRESORTED_CATEGORIES_END = [MISCELLANEOUS_CATEGORY_NAME];

export const EXTERNAL_SOURCE_REDIRECT_URL = 'https://docs.datahub.com/docs/metadata-ingestion/cli-ingestion';

export const CARD_HEIGHT = 94;
export const CARD_WIDTH = 318;

export function groupByCategory(sources: SourceConfig[]): Record<string, SourceConfig[]> {
    return sources.reduce<Record<string, SourceConfig[]>>((acc, src) => {
        const cat = src.category || 'Other';
        if (!acc[cat]) acc[cat] = [];
        acc[cat].push(src);
        return acc;
    }, {});
}

/**
 * Orders group entries by category following a hybrid sorting strategy:
 * 1. Predefined "start" categories (in PRESORTED_CATEGORIES_START order)
 * 2. Remaining categories sorted alphabetically
 * 3. Predefined "end" categories (in PRESORTED_CATEGORIES_END order)
 *
 * Filters out:
 * - Categories not present in input `groups`
 * - Categories with empty source arrays
 * - Categories exclusively in presorted arrays but missing from input
 */
export function getOrderedByCategoryEntriesOfGroups(
    groups: Record<string, SourceConfig[]>,
): [string, SourceConfig[]][] {
    // Step 1: Extract categories NOT in presorted arrays and sort alphabetically
    // - Filters out categories reserved for fixed start/end positions
    // - Sorts categories alphabetically
    const alphabeticalSortedCategories = Object.keys(groups)
        .filter(
            (category) =>
                !PRESORTED_CATEGORIES_START.includes(category) && !PRESORTED_CATEGORIES_END.includes(category),
        )
        .sort((a, b) => a.localeCompare(b));

    // Step 2: Construct final category order sequence
    // Order: [fixed-start categories] + [alphabetized middle] + [fixed-end categories]
    const categories = [...PRESORTED_CATEGORIES_START, ...alphabeticalSortedCategories, ...PRESORTED_CATEGORIES_END];

    // Step 3: Build result array
    // - Only includes categories present in input AND with non-empty sources
    const entries: [string, SourceConfig[]][] = [];
    categories.forEach((category) => {
        const sources = groups?.[category];
        if ((sources?.length ?? 0) > 0) {
            entries.push([category, sources]);
        }
    });

    return entries;
}

export function sortByPopularFirst(sources: SourceConfig[]) {
    return [...sources.filter((s) => s.isPopular), ...sources.filter((s) => !s.isPopular)];
}

/**
 * Sorts an array of source configurations by priority (ascending) and display name (alphabetically).
 *
 * Sorting rules:
 * 1. Priority-based sorting:
 *    - Sources with defined priority values are sorted first (lower numbers = higher priority)
 *    - Sources without priority (undefined) are placed after all prioritized sources
 * 2. Alphabetical fallback:
 *    - Sources with equal priority (or both undefined) are sorted by displayName
 */
export function sortByPriorityAndDisplayName(sources: SourceConfig[]) {
    return [...sources].sort((a, b) => {
        // Case 1: Both sources have defined priorities
        if (a.priority !== undefined && b.priority !== undefined) {
            const priorityDiff = a.priority - b.priority;
            // If priorities differ, sort by priority (ascending)
            if (priorityDiff !== 0) return priorityDiff;
            // If priorities are equal, sort alphabetically by displayName
            return a.displayName.localeCompare(b.displayName);
        }

        // Case 2: Only source A has priority - it comes first
        if (a.priority !== undefined) return -1;
        // Case 3: Only source B has priority - it comes first
        if (b.priority !== undefined) return 1;
        // Case 4: Neither has priority - sort alphabetically by displayName
        return a.displayName.localeCompare(b.displayName);
    });
}

export function computeRows(popular: SourceConfig[], nonPopular: SourceConfig[], cardsPerRow: number) {
    const effectiveColumns = Math.max(1, cardsPerRow);

    const visible: SourceConfig[] = [...popular];
    let hidden: SourceConfig[] = [];

    // Popular cards in the last row
    const usedSlotsInLastRow = visible.length % effectiveColumns;

    // Free slots in the last row
    const freeSlotsInLastRow = usedSlotsInLastRow === 0 ? 0 : effectiveColumns - usedSlotsInLastRow;

    const canStartInSameRow = freeSlotsInLastRow > 0;

    // Slots available to show non-popular, keeping one for show all card
    const slotsForNonPopular = canStartInSameRow ? Math.max(0, freeSlotsInLastRow - 1) : effectiveColumns - 1;

    const nonPopularToShow = nonPopular.slice(0, slotsForNonPopular);

    visible.push(...nonPopularToShow);
    hidden.push(...nonPopular.slice(slotsForNonPopular));

    // If only 1 is hidden, add it to visible
    if (hidden.length === 1) {
        visible.push(hidden[0]);
        hidden = [];
    }

    return { visible, hidden };
}

export function getPillLabel(source: SourceConfig): PillLabel | null {
    if (source.isNew) return PillLabel.New;
    if (source.isPopular) return PillLabel.Popular;
    if (source.isExternal) return PillLabel.External;
    return null;
}
