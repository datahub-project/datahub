import { Empty } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { Entity, Maybe } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';
import { Direction, FetchedEntity } from '../types';
import EntityEdge from './EntityEdge';

const LineageEdgesWrapper = styled.div`
    height: 225px;
    overflow: auto;
    padding: 0 20px 10px 20px;
`;

const EmptyWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 100%;
    background-color: ${ANTD_GRAY[3]};
`;

interface Props {
    entity?: Maybe<Entity>;
    lineageDirection: Direction;
    entitiesToAdd: Entity[];
    entitiesToRemove: Entity[];
    setEntitiesToAdd: React.Dispatch<React.SetStateAction<Entity[]>>;
    setEntitiesToRemove: React.Dispatch<React.SetStateAction<Entity[]>>;
}

export default function LineageEdges({
    entity,
    lineageDirection,
    entitiesToAdd,
    entitiesToRemove,
    setEntitiesToAdd,
    setEntitiesToRemove,
}: Props) {
    const entityRegistry = useEntityRegistry();

    let fetchedEntity: FetchedEntity | null | undefined = null;
    if (entity) {
        fetchedEntity = entityRegistry.getLineageVizConfig(entity?.type, entity);
    }

    const lineageChildren =
        lineageDirection === Direction.Upstream ? fetchedEntity?.upstreamChildren : fetchedEntity?.downstreamChildren;
    const urnsToRemove = entitiesToRemove.map((entityToRemove) => entityToRemove.urn);
    const filteredChildren = lineageChildren?.filter((child) => !urnsToRemove.includes(child.entity.urn));

    function removeEntity(removedEntity: Entity) {
        if (lineageChildren?.find((child) => child.entity.urn === removedEntity.urn)) {
            setEntitiesToRemove((existingEntitiesToRemove) => [...existingEntitiesToRemove, removedEntity]);
        } else {
            setEntitiesToAdd((existingEntitiesToAdd) =>
                existingEntitiesToAdd.filter((addedEntity) => addedEntity.urn !== removedEntity.urn),
            );
        }
    }

    return (
        <LineageEdgesWrapper>
            {!filteredChildren?.length && !entitiesToAdd.length && (
                <EmptyWrapper data-testid="empty-lineage">
                    <Empty description={`No ${lineageDirection.toLocaleLowerCase()} entities`} />
                </EmptyWrapper>
            )}
            {filteredChildren &&
                filteredChildren?.map((child) => (
                    <EntityEdge key={child.entity.urn} entity={child.entity} removeEntity={removeEntity} />
                ))}
            {entitiesToAdd.map((addedEntity) => (
                <EntityEdge key={addedEntity.urn} entity={addedEntity} removeEntity={removeEntity} />
            ))}
        </LineageEdgesWrapper>
    );
}
