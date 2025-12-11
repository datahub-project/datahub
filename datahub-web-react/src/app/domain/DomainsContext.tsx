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

export interface DomainsContextType {
    entityData: GenericEntityProperties | null;
    setEntityData: (entityData: GenericEntityProperties | null) => void;
    parentDomainsToUpdate: string[];
    setParentDomainsToUpdate: (values: string[]) => void;
}

export const DomainsContext = React.createContext<DomainsContextType>({
    entityData: null,
    setEntityData: () => {},
    parentDomainsToUpdate: [], // used to tell domains to refetch their children count after updates (create, move, delete)
    setParentDomainsToUpdate: () => {},
});

export const useDomainsContext = () => {
    const { entityData, setEntityData, parentDomainsToUpdate, setParentDomainsToUpdate } = useContext(DomainsContext);
    return { entityData, setEntityData, parentDomainsToUpdate, setParentDomainsToUpdate };
};
