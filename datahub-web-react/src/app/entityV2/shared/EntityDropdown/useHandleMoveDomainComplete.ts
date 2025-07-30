import { useApolloClient } from '@apollo/client';

import { EventType } from '@app/analytics';
import analytics from '@app/analytics/analytics';
import { removeFromListDomainsCache, updateListDomainsCache } from '@app/domain/utils';
import { useDomainsContext } from '@app/domainV2/DomainsContext';

import { Domain } from '@types';

export function useHandleMoveDomainComplete() {
    const client = useApolloClient();
    const { entityData } = useDomainsContext();

    const handleMoveDomainComplete = (urn: string, newParentUrn?: string) => {
        if (!entityData) return;

        const domain = entityData as Domain;
        const oldParentUrn = domain.parentDomains?.domains?.[0].urn;

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
    };

    return { handleMoveDomainComplete };
}
