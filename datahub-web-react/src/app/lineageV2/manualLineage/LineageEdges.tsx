import { Empty } from 'antd';
import React, { useContext, useMemo } from 'react';
import styled from 'styled-components/macro';
import { CorpUser, Entity, LineageDirection } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { getEdgeId, LineageNodesContext, setDifference } from '../common';
import EntityEdge from './EntityEdge';

const LineageEdgesWrapper = styled.div`
    padding: 0 20px 10px 20px;
    height: 100%;
`;

const EmptyWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 95%;
    background-color: ${ANTD_GRAY[3]};
    margin-top: 10px;
`;

interface Props {
    parentUrn: string;
    direction: LineageDirection;
    entitiesToAdd: Entity[];
    entitiesToRemove: Entity[];
    onRemoveEntity: (entity: Entity) => void;
}

export default function LineageEdges({ parentUrn, direction, entitiesToAdd, entitiesToRemove, onRemoveEntity }: Props) {
    const { nodes, edges, adjacencyList } = useContext(LineageNodesContext);

    const children = adjacencyList[direction].get(parentUrn) || new Set();
    const urnsToRemove = useMemo(
        () => new Set(entitiesToRemove.map((entityToRemove) => entityToRemove.urn)),
        [entitiesToRemove],
    );
    const filteredChildren = setDifference(children, urnsToRemove);

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
                        removeEntity={onRemoveEntity}
                        createdOn={edge?.created?.timestamp}
                        createdActor={edge?.created?.actor as CorpUser | undefined}
                    />
                );
            })}
            {entitiesToAdd.map((addedEntity) => (
                <EntityEdge key={addedEntity.urn} entity={addedEntity} removeEntity={onRemoveEntity} />
            ))}
        </LineageEdgesWrapper>
    );
}
