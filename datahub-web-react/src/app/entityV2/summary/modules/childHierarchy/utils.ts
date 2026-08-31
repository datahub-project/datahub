import i18next from 'i18next';

import { PageModuleFragment } from '@graphql/template.generated';
import { EntityType } from '@types';

export function getChildHierarchyModule(module: PageModuleFragment, urn: string, entityType: EntityType) {
    return {
        ...module,
        properties: {
            ...module.properties,
            name:
                entityType === EntityType.GlossaryNode
                    ? i18next.t('modules:childHierarchy.menu.contentsTitle')
                    : i18next.t('modules:childHierarchy.menu.domainsTitle'),
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
