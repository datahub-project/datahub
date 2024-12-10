import { useApolloClient } from '@apollo/client';
import { removeFromListDomainsCache, updateListDomainsCache } from '../../../domain/utils';
import { useDomainsContext } from '../../../domain/DomainsContext';
import { Domain } from '../../../../types.generated';
import analytics from '../../../analytics/analytics';
import { EventType } from '../../../analytics';

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
