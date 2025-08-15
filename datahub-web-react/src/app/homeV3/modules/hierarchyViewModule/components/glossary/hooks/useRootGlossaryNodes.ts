import { useMemo } from 'react';

import { useGetRootGlossaryNodesQuery } from '@graphql/glossary.generated';

export default function useRootGlossaryNodes() {
    const { data, loading } = useGetRootGlossaryNodesQuery();

    const glossaryNodes = useMemo(() => {
        if (data === undefined) return undefined;

        return data?.getRootGlossaryNodes?.nodes ?? [];
    }, [data]);

    return { data, glossaryNodes, loading };
}
