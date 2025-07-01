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
