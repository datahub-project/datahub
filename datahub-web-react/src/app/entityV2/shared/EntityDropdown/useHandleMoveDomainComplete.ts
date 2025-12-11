/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { UpdatedDomain, useDomainsContext } from '@app/domainV2/DomainsContext';

import { Domain } from '@types';

export function useHandleMoveDomainComplete() {
    const { entityData, setNewDomain, setDeletedDomain } = useDomainsContext();

    const handleMoveDomainComplete = (newParentUrn?: string) => {
        if (!entityData) return;

        const domain = entityData as Domain;
        const oldParentUrn = domain.parentDomains?.domains?.[0]?.urn;

        analytics.event({
            type: EventType.MoveDomainEvent,
            oldParentDomainUrn: oldParentUrn,
            parentDomainUrn: newParentUrn,
        });

        const deletedDomain: UpdatedDomain = {
            ...domain,
            parentDomain: oldParentUrn,
        };
        setDeletedDomain(deletedDomain);

        const newDomain: UpdatedDomain = {
            ...domain,
            parentDomain: newParentUrn,
        };
        setNewDomain(newDomain);
    };

    return { handleMoveDomainComplete };
}
