import { LoadingOutlined } from '@ant-design/icons';
import React, { useContext, useEffect, useState } from 'react';
import { Button, message, Modal } from 'antd';
import styled from 'styled-components/macro';
import { toTitleCase } from '../../../graphql-mock/helper';
import { EventType } from '../../analytics';
import analytics from '../../analytics/analytics';
import { useUserContext } from '../../context/useUserContext';
import EntityRegistry from '../../entity/EntityRegistry';
import { Direction } from '../../lineage/types';
import { FetchStatus, LineageEntity, LineageNodesContext } from '../common';
import AddEntityEdge from './AddEntityEdge';
import LineageEntityView from './LineageEntityView';
import LineageEdges from './LineageEdges';
import { Entity, EntityType, LineageDirection, LineageEdge } from '../../../types.generated';
import { useUpdateLineageMutation } from '../../../graphql/mutations.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import updateNodeContext from './updateNodeContext';
import { useOnClickExpandLineage } from '../LineageEntityNode/useOnClickExpandLineage';

const ModalFooter = styled.div`
    display: flex;
    justify-content: space-between;
`;

const TitleText = styled.div`
    font-weight: bold;
`;

const StyledModal = styled(Modal)`
    .ant-modal-body {
        padding: 0;
    }
`;

const LoadingWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    height: 225px;
    font-size: 30px;
`;

interface Props {
    node: LineageEntity;
    direction: LineageDirection;
    closeModal: () => void;
    refetch?: () => void;
}

export default function ManageLineageModal({ node, direction, closeModal, refetch }: Props) {
    const nodeContext = useContext(LineageNodesContext);
    const expandOneLevel = useOnClickExpandLineage(node.urn, node.type, direction, false);
    const { user } = useUserContext();
    const entityRegistry = useEntityRegistry();
    const [entitiesToAdd, setEntitiesToAdd] = useState<Entity[]>([]);
    const [entitiesToRemove, setEntitiesToRemove] = useState<Entity[]>([]);
    const [updateLineage] = useUpdateLineageMutation();
    const fetchStatus = node.fetchStatus[direction];
    const loading = node.fetchStatus[direction] === FetchStatus.LOADING;

    useEffect(() => {
        if (fetchStatus === FetchStatus.UNFETCHED) {
            expandOneLevel();
        }
    }, [fetchStatus, expandOneLevel]);

    function saveLineageChanges() {
        const payload = buildUpdateLineagePayload(direction, entitiesToAdd, entitiesToRemove, node.urn);
        updateLineage({ variables: { input: payload } })
            .then((res) => {
                if (res.data?.updateLineage) {
                    closeModal();
                    message.success('Updated lineage, refetching graph!');
                    updateNodeContext(node.urn, direction, user, nodeContext, entitiesToAdd, entitiesToRemove);
                    refetch?.();

                    recordAnalyticsEvents({
                        direction,
                        entitiesToAdd,
                        entitiesToRemove,
                        entityRegistry,
                        entityType: node.type,
                        entityPlatform: entityRegistry.getDisplayName(EntityType.DataPlatform, node.entity?.platform),
                    });
                }
            })
            .catch((e) => {
                message.error('Error updating lineage');
                console.warn(e);
            });
    }

    const isSaveDisabled = !entitiesToAdd.length && !entitiesToRemove.length;

    return (
        <StyledModal
            title={<TitleText>Manage {toTitleCase(direction.toLocaleLowerCase())} Lineage</TitleText>}
            onCancel={closeModal}
            keyboard
            open
            footer={
                <ModalFooter>
                    <Button onClick={closeModal} type="text">
                        Cancel
                    </Button>
                    <Button onClick={saveLineageChanges} disabled={isSaveDisabled}>
                        Save Changes
                    </Button>
                </ModalFooter>
            }
        >
            {node.entity && <LineageEntityView entity={node.entity} />}
            <AddEntityEdge
                direction={direction}
                setEntitiesToAdd={setEntitiesToAdd}
                entitiesToAdd={entitiesToAdd}
                entityUrn={node.urn}
                entityType={node.type}
            />
            {!loading && (
                <LineageEdges
                    parentUrn={node.urn}
                    direction={direction}
                    entitiesToAdd={entitiesToAdd}
                    entitiesToRemove={entitiesToRemove}
                    setEntitiesToAdd={setEntitiesToAdd}
                    setEntitiesToRemove={setEntitiesToRemove}
                />
            )}
            {loading && (
                <LoadingWrapper>
                    <LoadingOutlined />
                </LoadingWrapper>
            )}
        </StyledModal>
    );
}

interface AnalyticsEventsProps {
    direction: LineageDirection;
    entitiesToAdd: Entity[];
    entitiesToRemove: Entity[];
    entityRegistry: EntityRegistry;
    entityType?: EntityType;
    entityPlatform?: string;
}

function recordAnalyticsEvents({
    direction,
    entitiesToAdd,
    entitiesToRemove,
    entityRegistry,
    entityType,
    entityPlatform,
}: AnalyticsEventsProps) {
    entitiesToAdd.forEach((entityToAdd) => {
        const genericProps = entityRegistry.getGenericEntityProperties(entityToAdd.type, entityToAdd);
        analytics.event({
            type: EventType.ManuallyCreateLineageEvent,
            direction: directionFromLineageDirection(direction),
            sourceEntityType: entityType,
            sourceEntityPlatform: entityPlatform,
            destinationEntityType: entityToAdd.type,
            destinationEntityPlatform: genericProps?.platform?.name,
        });
    });
    entitiesToRemove.forEach((entityToRemove) => {
        const genericProps = entityRegistry.getGenericEntityProperties(entityToRemove.type, entityToRemove);
        analytics.event({
            type: EventType.ManuallyDeleteLineageEvent,
            direction: directionFromLineageDirection(direction),
            sourceEntityType: entityType,
            sourceEntityPlatform: entityPlatform,
            destinationEntityType: entityToRemove.type,
            destinationEntityPlatform: genericProps?.platform?.name,
        });
    });
}

function buildUpdateLineagePayload(
    lineageDirection: LineageDirection,
    entitiesToAdd: Entity[],
    entitiesToRemove: Entity[],
    entityUrn: string,
) {
    let edgesToAdd: LineageEdge[] = [];
    let edgesToRemove: LineageEdge[] = [];

    if (lineageDirection === LineageDirection.Upstream) {
        edgesToAdd = entitiesToAdd.map((entity) => ({ upstreamUrn: entity.urn, downstreamUrn: entityUrn }));
        edgesToRemove = entitiesToRemove.map((entity) => ({ upstreamUrn: entity.urn, downstreamUrn: entityUrn }));
    }
    if (lineageDirection === LineageDirection.Downstream) {
        edgesToAdd = entitiesToAdd.map((entity) => ({ upstreamUrn: entityUrn, downstreamUrn: entity.urn }));
        edgesToRemove = entitiesToRemove.map((entity) => ({ upstreamUrn: entityUrn, downstreamUrn: entity.urn }));
    }

    return { edgesToAdd, edgesToRemove };
}

function directionFromLineageDirection(lineageDirection: LineageDirection): Direction {
    return lineageDirection === LineageDirection.Upstream ? Direction.Upstream : Direction.Downstream;
}
