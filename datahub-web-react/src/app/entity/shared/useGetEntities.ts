/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect, useState } from 'react';

import { useGetEntitiesQuery } from '@graphql/entity.generated';
import { Entity } from '@types';

export function useGetEntities(urns: string[]): Entity[] {
    const [verifiedUrns, setVerifiedUrns] = useState<string[]>([]);

    useEffect(() => {
        urns.forEach((urn) => {
            if (urn.startsWith('urn:li:') && !verifiedUrns.includes(urn)) {
                setVerifiedUrns((prevUrns) => [...prevUrns, urn]);
            }
        });
    }, [urns, verifiedUrns]);

    const { data } = useGetEntitiesQuery({ variables: { urns: verifiedUrns }, skip: !verifiedUrns.length });
    return (data?.entities || []) as Entity[];
}
