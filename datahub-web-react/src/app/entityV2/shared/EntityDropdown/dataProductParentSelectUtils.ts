import { DataProduct, EntityType } from '@types';

/** Exclude self and any result that already lists self as an ancestor. */
export function filterResultsForMove(entity: DataProduct, entityUrn: string): boolean {
    return (
        entity.urn !== entityUrn &&
        entity.type === EntityType.DataProduct &&
        !entity.parentDataProducts?.some((ancestor) => ancestor.urn === entityUrn)
    );
}
