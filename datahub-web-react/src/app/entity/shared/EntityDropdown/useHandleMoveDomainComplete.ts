/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useApolloClient } from '@apollo/client';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { useDomainsContext } from '@app/domain/DomainsContext';
import { removeFromListDomainsCache, updateListDomainsCache } from '@app/domain/utils';

import { Domain } from '@types';

export function useHandleMoveDomainComplete() {
    const client = useApolloClient();
    const { entityData, parentDomainsToUpdate, setParentDomainsToUpdate } = useDomainsContext();

    const handleMoveDomainComplete = (urn: string, newParentUrn?: string) => {
        if (!entityData) return;

        const domain = entityData as Domain;
        const oldParentUrn = domain.parentDomains?.domains?.length ? domain.parentDomains.domains[0].urn : undefined;

        analytics.event({
            type: EventType.MoveDomainEvent,
            oldParentDomainUrn: oldParentUrn,
            parentDomainUrn: newParentUrn,
        });

        removeFromListDomainsCache(client, urn, 1, 1000, oldParentUrn);
        updateListDomainsCache(
            client,
            domain.urn,
            undefined,
            domain.properties?.name ?? '',
            domain.properties?.description ?? '',
            newParentUrn,
        );
        const newParentDomainsToUpdate = [...parentDomainsToUpdate];
        if (oldParentUrn) newParentDomainsToUpdate.push(oldParentUrn);
        if (newParentUrn) newParentDomainsToUpdate.push(newParentUrn);
        setParentDomainsToUpdate(newParentDomainsToUpdate);
    };

    return { handleMoveDomainComplete };
}
