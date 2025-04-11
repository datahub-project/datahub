import React from 'react';
import { Modal, Typography } from 'antd';
import { DataContract } from '../../../../../../../../types.generated';
import ClickOutside from '../../../../../../../shared/ClickOutside';
import { DataContractBuilderState } from './types';
import { DataContractBuilder } from './DataContractBuilder';

const modalStyle = {};
const modalBodyStyle = {
    paddingRight: 0,
    paddingLeft: 0,
    paddingBottom: 20,
    paddingTop: 0,
    maxHeight: '70vh',
    'overflow-x': 'auto',
};

type Props = {
    entityUrn: string;
    initialState?: DataContractBuilderState;
    onSubmit?: (contract: DataContract) => void;
    onCancel?: () => void;
};

/**
 * This component is a modal used for constructing new Data Contracts
 */
export const DataContractBuilderModal = ({ entityUrn, initialState, onSubmit, onCancel }: Props) => {
    const isEditing = initialState !== undefined;
    const titleText = isEditing ? 'Edit Data Contract' : 'New Data Contract';

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
        <ClickOutside onClickOutside={modalClosePopup} wrapperClassName="data-contract-builder-modal">
            <Modal
                wrapClassName="data-contract-builder-modal"
                width={800}
                footer={null}
                title={<Typography.Text>{titleText}</Typography.Text>}
                style={modalStyle}
                bodyStyle={modalBodyStyle}
                visible
                onCancel={onCancel}
            >
                <DataContractBuilder
                    entityUrn={entityUrn}
                    initialState={initialState}
                    onSubmit={onSubmit}
                    onCancel={onCancel}
                />
            </Modal>
        </ClickOutside>
    );
};
