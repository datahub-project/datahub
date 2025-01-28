import { Empty } from 'antd';
import React, { useContext, useMemo } from 'react';
import styled from 'styled-components/macro';
import { CorpUser, Entity, LineageDirection } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { getEdgeId, LineageNodesContext, setDifference } from '../common';
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
    parentUrn: string;
    direction: LineageDirection;
    entitiesToAdd: Entity[];
    entitiesToRemove: Entity[];
    setEntitiesToAdd: React.Dispatch<React.SetStateAction<Entity[]>>;
    setEntitiesToRemove: React.Dispatch<React.SetStateAction<Entity[]>>;
}

export default function LineageEdges({
    parentUrn,
    direction,
    entitiesToAdd,
    entitiesToRemove,
    setEntitiesToAdd,
    setEntitiesToRemove,
}: Props) {
    const { nodes, edges, adjacencyList } = useContext(LineageNodesContext);

    const children = adjacencyList[direction].get(parentUrn) || new Set();
    const urnsToRemove = useMemo(
        () => new Set(entitiesToRemove.map((entityToRemove) => entityToRemove.urn)),
        [entitiesToRemove],
    );
    const filteredChildren = setDifference(children, urnsToRemove);

    function removeEntity(removedEntity: Entity) {
        if (children.has(removedEntity.urn)) {
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
                    <Empty description={`No ${direction.toLocaleLowerCase()} entities`} />
                </EmptyWrapper>
            )}
            {filteredChildren?.map((childUrn) => {
                const edge = edges.get(getEdgeId(parentUrn, childUrn, direction));
                const childNode = nodes.get(childUrn);
                if (!childNode) return null;
                const backupEntity = { urn: childUrn, type: childNode.type };
                return (
                    <EntityEdge
                        key={childUrn}
                        entity={childNode.rawEntity || backupEntity}
                        removeEntity={removeEntity}
                        createdOn={edge?.created?.timestamp}
                        createdActor={edge?.created?.actor as CorpUser | undefined}
                    />
                );
            })}
            {entitiesToAdd.map((addedEntity) => (
                <EntityEdge key={addedEntity.urn} entity={addedEntity} removeEntity={removeEntity} />
            ))}
        </LineageEdgesWrapper>
    );
}
