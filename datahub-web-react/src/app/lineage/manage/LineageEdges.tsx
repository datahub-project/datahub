/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Empty } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import EntityEdge from '@app/lineage/manage/EntityEdge';
import { Direction, FetchedEntity } from '@app/lineage/types';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { CorpUser, Entity, Maybe } from '@types';

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

    const lineageRelationships =
        lineageDirection === Direction.Upstream
            ? fetchedEntity?.upstreamRelationships
            : fetchedEntity?.downstreamRelationships;
    const urnsToRemove = entitiesToRemove.map((entityToRemove) => entityToRemove.urn);
    const filteredRelationships = lineageRelationships?.filter(
        (relationship) => !urnsToRemove.includes(relationship.entity?.urn || ''),
    );

    function removeEntity(removedEntity: Entity) {
        if (lineageRelationships?.find((relationship) => relationship.entity?.urn === removedEntity.urn)) {
            setEntitiesToRemove((existingEntitiesToRemove) => [...existingEntitiesToRemove, removedEntity]);
        } else {
            setEntitiesToAdd((existingEntitiesToAdd) =>
                existingEntitiesToAdd.filter((addedEntity) => addedEntity.urn !== removedEntity.urn),
            );
        }
    }

    return (
        <LineageEdgesWrapper>
            {!filteredRelationships?.length && !entitiesToAdd.length && (
                <EmptyWrapper data-testid="empty-lineage">
                    <Empty description={`No ${lineageDirection.toLocaleLowerCase()} entities`} />
                </EmptyWrapper>
            )}
            {filteredRelationships &&
                filteredRelationships?.map((relationship) =>
                    relationship.entity ? (
                        <EntityEdge
                            key={relationship.entity.urn}
                            entity={relationship.entity}
                            removeEntity={removeEntity}
                            createdActor={relationship.createdActor as CorpUser}
                            createdOn={relationship.createdOn}
                        />
                    ) : null,
                )}
            {entitiesToAdd.map((addedEntity) => (
                <EntityEdge key={addedEntity.urn} entity={addedEntity} removeEntity={removeEntity} />
            ))}
        </LineageEdgesWrapper>
    );
}
