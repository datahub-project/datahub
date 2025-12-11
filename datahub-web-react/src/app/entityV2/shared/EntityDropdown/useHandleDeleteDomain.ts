/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { UpdatedDomain, useDomainsContext } from '@app/domainV2/DomainsContext';
import { GenericEntityProperties } from '@app/entity/shared/types';

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
