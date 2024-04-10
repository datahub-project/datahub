import Icon from '@ant-design/icons/lib/components/Icon';
import React, { useState } from 'react';
import styled from 'styled-components';
import { LineageDirection } from '../../../types.generated';
import { LineageEntity, onMouseDownCapturePreventSelect } from '../common';
import { ANTD_GRAY, LINEAGE_COLORS } from '../../entityV2/shared/constants';
import ManageLineageModal from '../manualLineage/ManageLineageModal';
import EditLineageIcon from '../icons/editLineage.svg?react';

const ButtonWrapper = styled.div<{ direction: LineageDirection }>`
    background-color: white;
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 10px;
    color: ${LINEAGE_COLORS.BLUE_1};
    cursor: pointer;
    display: flex;
    font-size: 18px;
    padding: 3px;
    position: absolute;
    top: 50%;
    transform: translateY(-50%);

    ${({ direction }) =>
        direction === LineageDirection.Upstream ? 'right: calc(100% - 5px);' : 'left: calc(100% - 5px);'}
`;

const Button = styled.span`
    border-radius: 20%;
    line-height: 0;

    :hover {
        background-color: ${LINEAGE_COLORS.BLUE_1}30;
    }
`;

interface Props {
    node: LineageEntity;
    direction: LineageDirection;
    refetch?: () => void;
}

export function EditLineageButton({ node, direction, refetch }: Props) {
    const [showModal, setShowModal] = useState(false);

    return (
        <>
            <ButtonWrapper direction={direction}>
                <Button onClick={() => setShowModal(true)} onMouseDownCapture={onMouseDownCapturePreventSelect}>
                    <Icon component={EditLineageIcon} />
                </Button>
            </ButtonWrapper>
            {showModal && (
                <ManageLineageModal
                    node={node}
                    direction={direction}
                    closeModal={() => setShowModal(false)}
                    refetch={refetch}
                />
            )}
        </>
    );
}
