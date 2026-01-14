import { SourceConfig } from '@app/ingestV2/source/builder/types';

export const MISCELLANEOUS_CATEGORY_NAME = 'Miscellaneous';

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

export function sortByPopularFirst(sources: SourceConfig[]) {
    return [...sources.filter((s) => s.isPopular), ...sources.filter((s) => !s.isPopular)];
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

export function getPillLabel(source: SourceConfig) {
    if (source.isPopular) return 'Popular';
    if (source.isExternal) return 'External';
    return '';
}
