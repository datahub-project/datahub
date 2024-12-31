import { Entity } from '../../../types.generated';
import EntityRegistry from '../../entity/EntityRegistry';
import { Direction, EntityAndType, FetchedEntities, FetchedEntity, NodeData, UpdatedLineages } from '../types';
import constructFetchedNode, { shouldIncludeChildEntity } from './constructFetchedNode';
import extendAsyncEntities from './extendAsyncEntities';

function createEntityAndType(entity: Entity) {
    return {
        type: entity.type,
        entity,
    } as EntityAndType;
}

function updateFetchedEntity(fetchedEntity: FetchedEntity, updatedLineages: UpdatedLineages) {
    if (!(fetchedEntity.urn in updatedLineages)) {
        return fetchedEntity;
    }
    const updatedLineage = updatedLineages[fetchedEntity.urn];
    let updatedEntity = fetchedEntity;
    const entitiesToAdd = updatedLineage.entitiesToAdd.map((entity) => createEntityAndType(entity));
    if (updatedLineage.lineageDirection === Direction.Upstream) {
        updatedEntity = {
            ...updatedEntity,
            upstreamChildren: [
                ...(updatedEntity.upstreamChildren || []).filter(
                    (child) => !updatedLineage.urnsToRemove.includes(child.entity.urn),
                ),
                ...entitiesToAdd.filter(
                    (entity) =>
                        !(updatedEntity.upstreamChildren || []).find((child) => child.entity.urn === entity.entity.urn),
                ),
            ],
        };
    } else {
        updatedEntity = {
            ...updatedEntity,
            downstreamChildren: [
                ...(updatedEntity.downstreamChildren || []).filter(
                    (child) => !updatedLineage.urnsToRemove.includes(child.entity.urn),
                ),
                ...entitiesToAdd.filter(
                    (entity) =>
                        !(updatedEntity.downstreamChildren || []).find(
                            (child) => child.entity.urn === entity.entity.urn,
                        ),
                ),
            ],
        };
    }

    return updatedEntity;
}

export default function constructTree(
    entityAndType: EntityAndType | null | undefined,
    fetchedEntities: FetchedEntities,
    direction: Direction,
    entityRegistry: EntityRegistry,
    updatedLineages: UpdatedLineages,
): NodeData {
    if (!entityAndType?.entity) return { name: 'loading...', children: [] };
    const constructedNodes = {};

    let updatedFetchedEntities = fetchedEntities;
    Array.from(updatedFetchedEntities.entries()).forEach(([urn, fetchedEntity]) => {
        if (urn in updatedLineages) {
            updatedFetchedEntities.set(urn, updateFetchedEntity(fetchedEntity, updatedLineages));
        }
    });
    Object.values(updatedLineages).forEach((updatedLineage) => {
        (updatedLineage as any).entitiesToAdd.forEach((entity) => {
            if (!updatedFetchedEntities.has(entity.urn)) {
                updatedFetchedEntities = extendAsyncEntities(
                    {},
                    {},
                    updatedFetchedEntities,
                    entityRegistry,
                    createEntityAndType(entity),
                    true,
                );
            }
        });
    });

    const fetchedEntity = entityRegistry.getLineageVizConfig(entityAndType.type, entityAndType.entity);
    const sibling = fetchedEntity?.siblings?.siblings?.[0];
    const fetchedSiblingEntity = sibling ? entityRegistry.getLineageVizConfig(sibling.type, sibling) : null;

    const root: NodeData = {
        name: fetchedEntity?.name || '',
        expandedName: fetchedEntity?.expandedName || '',
        urn: fetchedEntity?.urn,
        type: fetchedEntity?.type,
        subtype: fetchedEntity?.subtype,
        icon: fetchedEntity?.icon,
        platform: fetchedEntity?.platform,
        unexploredChildren: 0,
        siblingPlatforms: fetchedEntity?.siblingPlatforms,
        schemaMetadata: fetchedEntity?.schemaMetadata,
        inputFields: fetchedEntity?.inputFields,
        canEditLineage: fetchedEntity?.canEditLineage,
        upstreamRelationships: fetchedEntity?.upstreamRelationships || [],
        downstreamRelationships: fetchedEntity?.downstreamRelationships || [],
        health: fetchedEntity?.health,
        structuredProperties: fetchedEntity?.structuredProperties,
        siblingStructuredProperties: fetchedSiblingEntity?.structuredProperties,
    };
    const lineageConfig = entityRegistry.getLineageVizConfig(entityAndType.type, entityAndType.entity);
    let updatedLineageConfig = { ...lineageConfig };
    if (lineageConfig && lineageConfig.urn in updatedLineages) {
        updatedLineageConfig = updateFetchedEntity(lineageConfig, updatedLineages);
    }
    let children: EntityAndType[] = [];
    if (direction === Direction.Upstream) {
        children = updatedLineageConfig?.upstreamChildren || [];
    }
    if (direction === Direction.Downstream) {
        children = updatedLineageConfig?.downstreamChildren || [];
    }

    root.children = children
        .map((child) => {
            if (child.entity.urn === root.urn) {
                return null;
            }
            return constructFetchedNode(child.entity.urn, updatedFetchedEntities, direction, constructedNodes, [
                root.urn || '',
            ]);
        })
        ?.filter((child) => {
            const childEntity = updatedFetchedEntities.get(child?.urn || '');
            return shouldIncludeChildEntity(direction, children, childEntity, fetchedEntity);
        })
        ?.filter(Boolean) as Array<NodeData>;
    return root;
}
