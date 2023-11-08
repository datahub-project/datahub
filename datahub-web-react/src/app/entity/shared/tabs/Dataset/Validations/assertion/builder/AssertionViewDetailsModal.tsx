import React from 'react';
import styled from 'styled-components';
import { Button, Modal, Typography } from 'antd';
import { Assertion } from '../../../../../../../../types.generated';
import { AssertionDetails } from './details/AssertionDetails';
import { ANTD_GRAY } from '../../../../../constants';

const modalBodyStyle = { paddingRight: 48, paddingLeft: 48, paddingBottom: 20 };

const TitleContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

const DisabledInputStyles = styled.div`
    &&& .ant-select-disabled > .ant-select-selector,
    .ant-input-disabled,
    .ant-input-number-disabled {
        color: ${ANTD_GRAY[8]};
    }
`;

type Props = {
    assertion: Assertion;
    onCancel?: () => void;
};

/**
 * This component is used to view assertion details in read-only mode.
 */
export const AssertionViewDetailsModal = ({ assertion, onCancel }: Props) => (
    <Modal
        width={840}
        footer={
            <Button type="primary" onClick={onCancel}>
                Close
            </Button>
        }
        title={
            <TitleContainer>
                <Typography.Text>Assertion Details</Typography.Text>
            </TitleContainer>
        }
        bodyStyle={modalBodyStyle}
        onCancel={onCancel}
        open
    >
        <DisabledInputStyles>
            <AssertionDetails assertion={assertion} />
        </DisabledInputStyles>
    </Modal>
);
