import { buildEntityCache } from '@src/app/entityV2/view/builder/utils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { Entity, EntityType, GlossaryNode, GlossaryTerm } from '@src/types.generated';
import { useEffect, useState } from 'react';

type Option = {
    value: string;
    label: string;
    id: string;
    isParent: boolean;
    parentId?: string;
    parentValue?: string;
    entity: GlossaryTerm | GlossaryNode;
};

export const useGlossaryOptionsBuilder = (resolvedEntitiesData: any) => {
    const [initialOptions, setInitialOptions] = useState<Option[]>([]);
    const [entityCache, setEntityCache] = useState<Map<string, Entity>>(new Map());
    const entityRegistry = useEntityRegistryV2();

    // Helper to create option object from entity
    const createOption = (entity: GlossaryTerm | GlossaryNode, parentNodeUrn?: string): Option => ({
        value: entity.urn,
        label: entityRegistry.getDisplayName(entity.type, entity),
        id: entity.urn,
        isParent: entity.type === EntityType.GlossaryNode,
        parentId: parentNodeUrn,
        parentValue: parentNodeUrn,
        entity,
    });

    // Helper to process parent entities
    const processGlossaryNodeEntities = (entities: Array<GlossaryTerm | GlossaryNode>) => {
        const nodeEntities: GlossaryNode[] = [];
        const alreadyAddedUrns: string[] = [];
        const options: Option[] = entities.map((entity) => {
            const parentNodeUrn = entity.parentNodes?.nodes?.[0]?.urn;
            alreadyAddedUrns.push(entity.urn);

            if (entity.type === EntityType.GlossaryNode) {
                nodeEntities.push(entity as GlossaryNode);
            }

            return createOption(entity, parentNodeUrn);
        });

        return { options, nodeEntities, alreadyAddedUrns };
    };

    // Helper to process child entities of parent nodes
    const buildChildTermOptions = (nodeEntities: GlossaryNode[], alreadyAddedUrns: string[]) => {
        const childOptions: Option[] = [];
        nodeEntities.forEach((parentEntity: any) => {
            parentEntity?.children?.relationships.forEach((relationship) => {
                const { entity } = relationship;
                const { urn } = entity;

                if (!alreadyAddedUrns.includes(urn)) {
                    alreadyAddedUrns.push(urn);
                    childOptions.push(createOption(entity, parentEntity?.urn));
                }
            });
        });
        return childOptions;
    };

    // Helper to build options and cache from resolved entities
    const buildOptionsAndCache = (entities: Array<GlossaryTerm | GlossaryNode>) => {
        const { options, nodeEntities, alreadyAddedUrns } = processGlossaryNodeEntities(entities);
        setInitialOptions(options);

        const childOptions = buildChildTermOptions(nodeEntities, alreadyAddedUrns);

        const allOptions = [...options, ...childOptions];
        setEntityCache(buildEntityCache(entities)); // Assuming `buildEntityCache` is a utility function
    };

    useEffect(() => {
        if (resolvedEntitiesData?.entities?.length) {
            const { entities } = resolvedEntitiesData;
            buildOptionsAndCache(entities);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [resolvedEntitiesData, entityRegistry]);

    return { initialOptions, entityCache };
};
