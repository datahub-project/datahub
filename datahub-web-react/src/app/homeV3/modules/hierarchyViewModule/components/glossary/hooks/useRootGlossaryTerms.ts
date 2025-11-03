import { useMemo } from 'react';

import { useGetRootGlossaryTermsQuery } from '@graphql/glossary.generated';

export default function useRootGlossaryTerms() {
    const { data, loading } = useGetRootGlossaryTermsQuery({
        fetchPolicy: 'cache-and-network',
        nextFetchPolicy: 'cache-first',
    });

    const glossaryTerms = useMemo(() => {
        if (data === undefined) return undefined;

        return data?.getRootGlossaryTerms?.terms ?? [];
    }, [data]);

    return { data, glossaryTerms, loading };
}
