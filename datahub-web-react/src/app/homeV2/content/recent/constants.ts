/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { EntityType } from '@types';

export const ENTITY_TYPES = [
    EntityType.Dataset,
    EntityType.Chart,
    EntityType.Dashboard,
    EntityType.DataFlow,
    EntityType.DataJob,
    EntityType.Container,
    EntityType.Domain,
    EntityType.GlossaryTerm,
    EntityType.GlossaryNode,
    EntityType.Notebook,
    EntityType.Mlfeature,
    EntityType.MlfeatureTable,
    EntityType.Mlmodel,
    EntityType.MlmodelGroup,
    EntityType.MlprimaryKey,
];
