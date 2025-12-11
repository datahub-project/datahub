/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect, useMemo, useState } from 'react';

import { useGetEntitiesQuery } from '@graphql/entity.generated';
import { Entity } from '@types';

export function useGetEntities(
    urns: string[],
    checkForExistence?: boolean,
): {
    entities: Entity[];
    loading: boolean;
} {
    const verifiedUrns = useMemo(
        () => urns.filter((urn) => typeof urn === 'string' && urn.startsWith('urn:li:')),
        [urns],
    );

    const { data, loading } = useGetEntitiesQuery({
        variables: { urns: verifiedUrns, checkForExistence },
        skip: !verifiedUrns.length,
        fetchPolicy: 'cache-first',
    });

    const [entities, setEntities] = useState<Entity[]>([]);

    useEffect(() => {
        if (data?.entities && data.entities.length > 0) {
            setEntities(data.entities as Entity[]);
        } else if (!loading && (!data?.entities || data.entities.length === 0)) {
            setEntities([]);
        }
    }, [data, loading]);

    return { entities, loading };
}
