/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useMemo } from 'react';

import { useGetRootGlossaryNodesQuery } from '@graphql/glossary.generated';

export default function useRootGlossaryNodes() {
    const { data, loading } = useGetRootGlossaryNodesQuery({
        fetchPolicy: 'cache-and-network',
        nextFetchPolicy: 'cache-first',
    });

    const glossaryNodes = useMemo(() => {
        if (data === undefined) return undefined;

        return data?.getRootGlossaryNodes?.nodes ?? [];
    }, [data]);

    return { data, glossaryNodes, loading };
}
