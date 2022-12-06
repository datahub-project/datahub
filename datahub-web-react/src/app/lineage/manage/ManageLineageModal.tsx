import { LoadingOutlined } from '@ant-design/icons';
import { Button, Modal } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { useGetEntityLineageQuery } from '../../../graphql/lineage.generated';
import { Direction } from '../types';
import LineageEntityView from './LineageEntityView';

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
            {!loading && <>{data?.entity && <LineageEntityView entity={data.entity} />}</>}
            {/* TODO: Add entity search to add to lineage */}
            {/* TODO: Add list of entities in lineage */}
        </StyledModal>
    );
}
