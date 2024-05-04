import { useEffect, useState } from 'react';
import { useGetEntitiesQuery } from '../../../graphql/entity.generated';

export interface PropagationContext {
    propagated?: boolean;
    origin?: string;
    actor?: string;
}

export function usePropagationContextEntities(contextObj?: PropagationContext | null) {
    const [urns, setUrns] = useState<string[]>([]);
    const propagationOriginUrn = contextObj?.origin;
    const propagationActorUrn = contextObj?.actor;

    useEffect(() => {
        if (propagationOriginUrn && propagationOriginUrn.startsWith('urn:li') && !urns.includes(propagationOriginUrn)) {
            setUrns([...urns, propagationOriginUrn]);
        }
        if (propagationActorUrn && propagationActorUrn.startsWith('urn:li') && !urns.includes(propagationActorUrn)) {
            setUrns([...urns, propagationActorUrn]);
        }
    }, [propagationOriginUrn, propagationActorUrn, urns]);

    const { data } = useGetEntitiesQuery({ variables: { urns }, skip: !urns.length });
    const originEntity = data?.entities?.find((e) => e?.urn === propagationOriginUrn);
    const actorEntity = data?.entities?.find((e) => e?.urn === propagationActorUrn);

    return { originEntity, actorEntity };
}
