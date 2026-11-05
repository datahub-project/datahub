import { UpdatedDomain, useDomainsContext } from '@app/domainV2/DomainsContext';
import { GenericEntityProperties } from '@app/entityV2/shared/types';

import { EntityType } from '@types';

interface DeleteDomainProps {
    entityData: GenericEntityProperties;
    urn: string;
}

export function useHandleDeleteDomain({ entityData, urn }: DeleteDomainProps) {
    const { setDeletedDomain } = useDomainsContext();

    const handleDeleteDomain = () => {
        const parentDomainUrn = entityData?.parentDomains?.domains?.[0]?.urn || undefined;
        const deletedDomain: UpdatedDomain = {
            urn,
            type: EntityType.Domain,
            id: urn,
            parentDomain: parentDomainUrn,
        };
        setDeletedDomain(deletedDomain);
    };

    return { handleDeleteDomain };
}
