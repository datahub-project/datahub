import { LoadingOutlined } from '@ant-design/icons';
import { Button, Modal } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { useGetEntityLineageQuery } from '../../../graphql/lineage.generated';
import { Direction } from '../types';
import AddEntityEdge from './AddEntityEdge';
import LineageEntityView from './LineageEntityView';
import LineageEdges from './LineageEdges';
import { Entity } from '../../../types.generated';

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
}

export default function ManageLineageModal({ entityUrn, lineageDirection, closeModal }: Props) {
    const [entitiesToAdd, setEntitiesToAdd] = useState<Entity[]>([]);
    const [urnsToRemove, setUrnsToRemove] = useState<string[]>([]);

    const { data, loading } = useGetEntityLineageQuery({
        variables: {
            urn: entityUrn,
            showColumns: false,
            excludeDownstream: lineageDirection === Direction.Upstream,
            excludeUpstream: lineageDirection === Direction.Downstream,
        },
    });

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
                    <Button onClick={() => {}}>Save Changes</Button>
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
