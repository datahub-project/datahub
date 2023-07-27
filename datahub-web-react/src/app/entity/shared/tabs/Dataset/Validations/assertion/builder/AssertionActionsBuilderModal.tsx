import React from 'react';
import styled from 'styled-components';
import { Modal, Typography } from 'antd';
import { Assertion } from '../../../../../../../../types.generated';
import ClickOutside from '../../../../../../../shared/ClickOutside';
import { AssertionActionsBuilder } from './AssertionActionsBuilder';

const modalStyle = {};
const modalBodyStyle = { paddingRight: 48, paddingLeft: 48, paddingBottom: 20 };

const TitleContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

type Props = {
    urn: string;
    assertion: Assertion;
    onSubmit?: (result: Assertion) => void;
    onCancel?: () => void;
};

/**
 * This component is used to edit an assertion actions, e.g. to change whether an incident is raised when an assertion fails.
 */
export const AssertionActionsBuilderModal = ({ urn, assertion, onSubmit, onCancel }: Props) => {
    const modalClosePopup = () => {
        Modal.confirm({
            title: 'Exit Editor',
            content: `Are you sure you want to exit the editor? All changes will be lost`,
            onOk() {
                onCancel?.();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <ClickOutside onClickOutside={modalClosePopup} wrapperClassName="assertion-actions-builder-modal">
            <Modal
                wrapClassName="assertion-actions-builder-modal"
                width={840}
                footer={null}
                title={
                    <TitleContainer>
                        <Typography.Text>Manage Assertion</Typography.Text>
                    </TitleContainer>
                }
                style={modalStyle}
                bodyStyle={modalBodyStyle}
                visible
                onCancel={onCancel}
            >
                <AssertionActionsBuilder urn={urn} assertion={assertion} onSubmit={onSubmit} onCancel={onCancel} />
            </Modal>
        </ClickOutside>
    );
};
