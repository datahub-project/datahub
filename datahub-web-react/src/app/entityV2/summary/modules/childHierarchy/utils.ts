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
