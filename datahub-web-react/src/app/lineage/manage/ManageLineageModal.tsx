import { LoadingOutlined } from '@ant-design/icons';
import { Button, message, Modal } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { useGetEntityLineageQuery } from '../../../graphql/lineage.generated';
import { Direction, UpdatedLineages } from '../types';
import AddEntityEdge from './AddEntityEdge';
import LineageEntityView from './LineageEntityView';
import LineageEdges from './LineageEdges';
import { Entity, LineageEdge } from '../../../types.generated';
import { useUpdateLineageMutation } from '../../../graphql/mutations.generated';

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
    height: 350px;
    font-size: 30px;
`;

export function buildUpdateLineagePayload(
    lineageDirection: Direction,
    entitiesToAdd: Entity[],
    urnsToRemove: string[],
    entityUrn: string,
) {
    let edgesToAdd: LineageEdge[] = [];
    let edgesToRemove: LineageEdge[] = [];

    if (lineageDirection === Direction.Upstream) {
        edgesToAdd = entitiesToAdd.map((entity) => ({ upstreamUrn: entity.urn, downstreamUrn: entityUrn }));
        edgesToRemove = urnsToRemove.map((urn) => ({ upstreamUrn: urn, downstreamUrn: entityUrn }));
    }
    if (lineageDirection === Direction.Downstream) {
        edgesToAdd = entitiesToAdd.map((entity) => ({ upstreamUrn: entityUrn, downstreamUrn: entity.urn }));
        edgesToRemove = urnsToRemove.map((urn) => ({ upstreamUrn: entityUrn, downstreamUrn: urn }));
    }

    return { edgesToAdd, edgesToRemove };
}

interface Props {
    entityUrn: string;
    lineageDirection: Direction;
    closeModal: () => void;
    refetchEntity: () => void;
    setUpdatedLineages: React.Dispatch<React.SetStateAction<UpdatedLineages>>;
}

export default function ManageLineageModal({
    entityUrn,
    lineageDirection,
    closeModal,
    refetchEntity,
    setUpdatedLineages,
}: Props) {
    const [entitiesToAdd, setEntitiesToAdd] = useState<Entity[]>([]);
    const [urnsToRemove, setUrnsToRemove] = useState<string[]>([]);
    const [updateLineage] = useUpdateLineageMutation();

    const { data, loading } = useGetEntityLineageQuery({
        variables: {
            urn: entityUrn,
            showColumns: false,
            excludeDownstream: lineageDirection === Direction.Upstream,
            excludeUpstream: lineageDirection === Direction.Downstream,
        },
    });

    function saveLineageChanges() {
        const payload = buildUpdateLineagePayload(lineageDirection, entitiesToAdd, urnsToRemove, entityUrn);
        updateLineage({ variables: { input: payload } })
            .then((res) => {
                if (res.data?.updateLineage) {
                    closeModal();
                    message.success('Updated lineage!');
                    setTimeout(() => {
                        refetchEntity();
                    }, 2000);

                    setUpdatedLineages((updatedLineages) => ({
                        ...updatedLineages,
                        [entityUrn]: {
                            lineageDirection,
                            entitiesToAdd,
                            urnsToRemove,
                        },
                    }));
                }
            })
            .catch(() => {
                message.error('Error updating lineage');
            });
    }

    const isSaveDisabled = !entitiesToAdd.length && !urnsToRemove.length;

    return (
        <StyledModal
            title={<TitleText>Manage {lineageDirection} Lineage</TitleText>}
            visible
            onCancel={closeModal}
            keyboard
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
            {loading && (
                <LoadingWrapper>
                    <LoadingOutlined />
                </LoadingWrapper>
            )}
            {!loading && (
                <>
                    {data?.entity && <LineageEntityView entity={data.entity} />}
                    <AddEntityEdge
                        lineageDirection={lineageDirection}
                        setEntitiesToAdd={setEntitiesToAdd}
                        entitiesToAdd={entitiesToAdd}
                    />
                    <LineageEdges
                        entity={data?.entity}
                        lineageDirection={lineageDirection}
                        entitiesToAdd={entitiesToAdd}
                        urnsToRemove={urnsToRemove}
                        setEntitiesToAdd={setEntitiesToAdd}
                        setUrnsToRemove={setUrnsToRemove}
                    />
                </>
            )}
        </StyledModal>
    );
}
