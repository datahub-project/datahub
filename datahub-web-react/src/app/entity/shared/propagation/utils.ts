import { useGetEntities } from '@app/entity/shared/useGetEntities';

import { StringMapEntry } from '@types';

export function isPropagated(sourceDetail?: StringMapEntry[] | null): boolean {
    return !!sourceDetail?.find((mapEntry) => mapEntry.key === 'propagated' && mapEntry.value === 'true');
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
