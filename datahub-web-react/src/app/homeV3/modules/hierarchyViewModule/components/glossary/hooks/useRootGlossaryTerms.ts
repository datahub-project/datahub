/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
