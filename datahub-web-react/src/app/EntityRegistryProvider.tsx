/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import buildEntityRegistryV2 from '@app/buildEntityRegistryV2';
import EntityRegistry from '@app/entity/EntityRegistry';
import useBuildEntityRegistry from '@app/useBuildEntityRegistry';
import { EntityRegistryContext } from '@src/entityRegistryContext';

export const globalEntityRegistryV2 = buildEntityRegistryV2();

const EntityRegistryProvider = ({ children }: { children: React.ReactNode }) => {
    const entityRegistry = useBuildEntityRegistry() as EntityRegistry;
    return <EntityRegistryContext.Provider value={entityRegistry}>{children}</EntityRegistryContext.Provider>;
};

export default EntityRegistryProvider;
