/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { PageModuleFragment } from '@graphql/template.generated';
import { EntityType } from '@types';

export function getChildHierarchyModule(module: PageModuleFragment, urn: string, entityType: EntityType) {
    return {
        ...module,
        properties: {
            ...module.properties,
            name: entityType === EntityType.GlossaryNode ? 'Contents' : 'Domains',
            params: {
                ...module.properties.params,
                hierarchyViewParams: {
                    assetUrns: [urn],
                    showRelatedEntities: entityType === EntityType.GlossaryNode,
                    relatedEntitiesFilterJson:
                        entityType === EntityType.GlossaryNode
                            ? '{"type":"logical","operator":"and","operands":[]}'
                            : undefined, // empty filter, show all contents
                },
            },
        },
    };
}
