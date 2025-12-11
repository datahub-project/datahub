/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useApolloClient } from '@apollo/client';

import { useDomainsContext } from '@app/domain/DomainsContext';
import { removeFromListDomainsCache } from '@app/domain/utils';
import { GenericEntityProperties } from '@app/entity/shared/types';

interface DeleteDomainProps {
    entityData: GenericEntityProperties;
    urn: string;
}

export function useHandleDeleteDomain({ entityData, urn }: DeleteDomainProps) {
    const client = useApolloClient();
    const { parentDomainsToUpdate, setParentDomainsToUpdate } = useDomainsContext();

    const handleDeleteDomain = () => {
        if (entityData.parentDomains && entityData.parentDomains.domains.length > 0) {
            const parentDomainUrn = entityData.parentDomains.domains[0].urn;

            removeFromListDomainsCache(client, urn, 1, 1000, parentDomainUrn);
            setParentDomainsToUpdate([...parentDomainsToUpdate, parentDomainUrn]);
        } else {
            removeFromListDomainsCache(client, urn, 1, 1000);
        }
    };

    return { handleDeleteDomain };
}
