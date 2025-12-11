/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import EntityRegistryV1 from '@app/entity/EntityRegistry';
import EntityRegistryV2 from '@app/entityV2/EntityRegistry';

export type EntityRegistry = EntityRegistryV1 | EntityRegistryV2;

export const EntityRegistryContext = React.createContext<EntityRegistryV1>(new EntityRegistryV1());
