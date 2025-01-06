import { useEffect, useState } from 'react';
import { useGetEntitiesQuery } from '../../../graphql/entity.generated';

export interface PropagationContext {
    propagated?: boolean;
    origin?: string;
    actor?: string;
    relationship?: string;
}

export function usePropagationContextEntities(contextObj?: PropagationContext | null) {
    const [urns, setUrns] = useState<string[]>([]);
    const propagationOriginUrn = contextObj?.origin;

    useEffect(() => {
        if (propagationOriginUrn && propagationOriginUrn.startsWith('urn:li') && !urns.includes(propagationOriginUrn)) {
            setUrns([...urns, propagationOriginUrn]);
        }
    }, [propagationOriginUrn, urns]);

    const { data } = useGetEntitiesQuery({ variables: { urns }, skip: !urns.length });
    const originEntity = data?.entities?.find((e) => e?.urn === propagationOriginUrn);

    return { originEntity };
}
