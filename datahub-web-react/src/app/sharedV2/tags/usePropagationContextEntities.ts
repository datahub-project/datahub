import { useEffect, useState } from 'react';
import { useGetEntitiesQuery } from '../../../graphql/entity.generated';
import { getSourceUrnFromSchemaFieldUrn } from '../../lineageV2/lineageUtils';

export interface PropagationContext {
    propagated?: boolean;
    origin?: string;
    actor?: string;
}

export function usePropagationContextEntities(contextObj?: PropagationContext | null) {
    const [urns, setUrns] = useState<string[]>([]);
    const propagationOriginUrn = contextObj?.origin;
    const finalOriginUrn = propagationOriginUrn?.startsWith('urn:li:schemaField')
        ? getSourceUrnFromSchemaFieldUrn(propagationOriginUrn)
        : propagationOriginUrn;
    const propagationActorUrn = contextObj?.actor;

    useEffect(() => {
        if (finalOriginUrn && finalOriginUrn.startsWith('urn:li') && !urns.includes(finalOriginUrn)) {
            setUrns([...urns, finalOriginUrn]);
        }
        if (propagationActorUrn && propagationActorUrn.startsWith('urn:li') && !urns.includes(propagationActorUrn)) {
            setUrns([...urns, propagationActorUrn]);
        }
    }, [finalOriginUrn, propagationActorUrn, urns]);

    const { data } = useGetEntitiesQuery({ variables: { urns }, skip: !urns.length });
    const originEntity = data?.entities?.find((e) => e?.urn === finalOriginUrn);
    const actorEntity = data?.entities?.find((e) => e?.urn === propagationActorUrn);

    return { originEntity, actorEntity };
}
