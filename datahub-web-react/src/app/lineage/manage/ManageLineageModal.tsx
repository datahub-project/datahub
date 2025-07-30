import { LoadingOutlined } from '@ant-design/icons';
import { Button, Modal, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import AddEntityEdge from '@app/lineage/manage/AddEntityEdge';
import LineageEdges from '@app/lineage/manage/LineageEdges';
import LineageEntityView from '@app/lineage/manage/LineageEntityView';
import { Direction, UpdatedLineages } from '@app/lineage/types';
import { buildUpdateLineagePayload, recordAnalyticsEvents } from '@app/lineage/utils/manageLineageUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetEntityLineageQuery } from '@graphql/lineage.generated';
import { useUpdateLineageMutation } from '@graphql/mutations.generated';
import { Entity, EntityType } from '@types';

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

interface Props {
    entityUrn: string;
    lineageDirection: Direction;
    closeModal: () => void;
    refetchEntity: () => void;
    setUpdatedLineages: React.Dispatch<React.SetStateAction<UpdatedLineages>>;
    showLoading?: boolean;
    entityType?: EntityType;
    entityPlatform?: string;
}

export default function ManageLineageModal({
    entityUrn,
    lineageDirection,
    closeModal,
    refetchEntity,
    setUpdatedLineages,
    showLoading,
    entityType,
    entityPlatform,
}: Props) {
    const entityRegistry = useEntityRegistry();
    const [entitiesToAdd, setEntitiesToAdd] = useState<Entity[]>([]);
    const [entitiesToRemove, setEntitiesToRemove] = useState<Entity[]>([]);
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
        const payload = buildUpdateLineagePayload(lineageDirection, entitiesToAdd, entitiesToRemove, entityUrn);
        updateLineage({ variables: { input: payload } })
            .then((res) => {
                if (res.data?.updateLineage) {
                    closeModal();
                    if (showLoading) {
                        message.loading('Loading...');
                    } else {
                        message.success('Updated lineage!');
                    }
                    setTimeout(() => {
                        refetchEntity();
                        if (showLoading) {
                            message.destroy();
                            message.success('Updated lineage!');
                        }
                    }, 2000);

                    setUpdatedLineages((updatedLineages) => ({
                        ...updatedLineages,
                        [entityUrn]: {
                            lineageDirection,
                            entitiesToAdd,
                            urnsToRemove: entitiesToRemove.map((entity) => entity.urn),
                        },
                    }));
                    recordAnalyticsEvents({
                        lineageDirection,
                        entitiesToAdd,
                        entitiesToRemove,
                        entityRegistry,
                        entityType,
                        entityPlatform,
                    });
                }
            })
            .catch((error) => {
                message.error(error.message || 'Error updating lineage');
            });
    }

    const isSaveDisabled = !entitiesToAdd.length && !entitiesToRemove.length;

    return (
        <StyledModal
            title={<TitleText>Manage {lineageDirection} Lineage</TitleText>}
            open
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
                        entityUrn={entityUrn}
                        entityType={entityType}
                    />
                    <LineageEdges
                        entity={data?.entity}
                        lineageDirection={lineageDirection}
                        entitiesToAdd={entitiesToAdd}
                        entitiesToRemove={entitiesToRemove}
                        setEntitiesToAdd={setEntitiesToAdd}
                        setEntitiesToRemove={setEntitiesToRemove}
                    />
                </>
            )}
        </StyledModal>
    );
}
