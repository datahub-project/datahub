import { StringMapEntry } from '../../../../types.generated';
import { useGetEntities } from '../useGetEntities';

export function usePropagationDetails(sourceDetail?: StringMapEntry[] | null) {
    const isPropagated = !!sourceDetail?.find((mapEntry) => mapEntry.key === 'propagated' && mapEntry.value === 'true');
    const originEntityUrn = sourceDetail?.find((mapEntry) => mapEntry.key === 'origin')?.value || '';
    const viaEntityUrn = sourceDetail?.find((mapEntry) => mapEntry.key === 'via')?.value || '';

    const entities = useGetEntities([originEntityUrn, viaEntityUrn]);
    const originEntity = entities.find((e) => e.urn === originEntityUrn);
    const viaEntity = entities.find((e) => e.urn === viaEntityUrn);

    return {
        isPropagated,
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
