import { useGetEntities } from '@app/entity/shared/useGetEntities';

import { StringMapEntry } from '@types';

export function isPropagated(sourceDetail?: StringMapEntry[] | null): boolean {
    return !!sourceDetail?.find((mapEntry) => mapEntry.key === 'propagated' && mapEntry.value === 'true');
}

// A tag/term/aspect is "external" when it was ingested from an external source
// (e.g. Lake Formation) rather than added in DataHub. Ingestion sources stamp
// the association's attribution with `external=true` plus an `origin` marker.
export function isExternal(sourceDetail?: StringMapEntry[] | null): boolean {
    return !!sourceDetail?.find((mapEntry) => mapEntry.key === 'external' && mapEntry.value === 'true');
}

export function getAttributionOrigin(sourceDetail?: StringMapEntry[] | null): string | undefined {
    return sourceDetail?.find((mapEntry) => mapEntry.key === 'origin')?.value || undefined;
}

// Turn a machine origin marker (e.g. "lake-formation") into a display label
// (e.g. "Lake Formation"). Left untouched when the origin is an entity URN,
// which is only used by the propagation path, not the external path.
export function formatAttributionOrigin(origin?: string): string | undefined {
    if (!origin) return undefined;
    return origin
        .split(/[-_\s]+/)
        .filter(Boolean)
        .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
        .join(' ');
}

export function usePropagationDetails(sourceDetail?: StringMapEntry[] | null) {
    const originEntityUrn = sourceDetail?.find((mapEntry) => mapEntry.key === 'origin')?.value || '';
    const viaEntityUrn = sourceDetail?.find((mapEntry) => mapEntry.key === 'via')?.value || '';

    const entities = useGetEntities([originEntityUrn, viaEntityUrn]);
    const originEntity = entities.find((e) => e.urn === originEntityUrn);
    const viaEntity = entities.find((e) => e.urn === viaEntityUrn);

    return {
        isPropagated: isPropagated(sourceDetail),
        origin: {
            urn: originEntityUrn,
            entity: originEntity,
        },
        via: {
            urn: viaEntityUrn,
            entity: viaEntity,
        },
    };
}
