import { useApolloClient } from '@apollo/client';
import { removeFromListDomainsCache, updateListDomainsCache } from '../../../domain/utils';
import { Domain } from '../../../../types.generated';
import analytics from '../../../analytics/analytics';
import { EventType } from '../../../analytics';
import { useDomainsContext } from '../../../domainV2/DomainsContext';

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
