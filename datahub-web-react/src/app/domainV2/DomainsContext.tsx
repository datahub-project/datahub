/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useContext } from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';

import { ListDomainFragment } from '@graphql/domain.generated';

export type UpdatedDomain = ListDomainFragment & { parentDomain?: string };

export interface DomainsContextType {
    entityData: GenericEntityProperties | null;
    setEntityData: (entityData: GenericEntityProperties | null) => void;
    newDomain: UpdatedDomain | null;
    setNewDomain: (newDomain: UpdatedDomain | null) => void;
    deletedDomain: UpdatedDomain | null;
    setDeletedDomain: (newDomain: UpdatedDomain | null) => void;
    updatedDomain: UpdatedDomain | null;
    setUpdatedDomain: (newDomain: UpdatedDomain | null) => void;
}

export const DomainsContext = React.createContext<DomainsContextType>({
    entityData: null,
    setEntityData: () => {},
    newDomain: null,
    setNewDomain: () => {},
    deletedDomain: null,
    setDeletedDomain: () => {},
    updatedDomain: null,
    setUpdatedDomain: () => {},
});

export const useDomainsContext = () => {
    return useContext(DomainsContext);
};
