import { Modal, Typography } from 'antd';
import React from 'react';

import { DataContractBuilder } from '@app/entity/shared/tabs/Dataset/Validations/contract/builder/DataContractBuilder';
import { DataContractBuilderState } from '@app/entity/shared/tabs/Dataset/Validations/contract/builder/types';
import ClickOutside from '@app/shared/ClickOutside';

import { DataContract, EntityType } from '@types';

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
    onPropose?: () => void;
    onCancel?: () => void;
    entityType?: EntityType;
};

/**
 * This component is a modal used for constructing new Data Contracts
 */
export const DataContractBuilderModal = ({
    entityUrn,
    initialState,
    onSubmit,
    onPropose,
    onCancel,
    entityType,
}: Props) => {
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
                open
                onCancel={onCancel}
            >
                <DataContractBuilder
                    entityUrn={entityUrn}
                    entityType={entityType}
                    initialState={initialState}
                    onSubmit={onSubmit}
                    onPropose={onPropose}
                    onCancel={onCancel}
                />
            </Modal>
        </ClickOutside>
    );
};
