import { mergeUrnEntities } from '@app/sharedV2/utils/mergeUrnEntities';

/** Merge one scrollAcrossEntities page into accumulated results (browse + flat search). */
export function mergeScrollPageResults<T extends { urn: string }>({
    current,
    fresh,
    scrollId,
}: {
    current: T[];
    fresh: T[];
    scrollId: string | null;
}): T[] {
    return mergeUrnEntities(current, fresh, scrollId === null);
}
