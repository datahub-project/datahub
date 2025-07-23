import { Modal } from '@components';
import React, { useState } from 'react';

import { DataContractBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/DataContractBuilder';
import { DataContractBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/types';
import ClickOutside from '@app/shared/ClickOutside';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { DataContract } from '@types';

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

    const [showConfirmationModal, setShowConfirmationModal] = useState(false);

    return (
        <ClickOutside
            onClickOutside={() => setShowConfirmationModal(true)}
            wrapperClassName="data-contract-builder-modal"
        >
            <Modal
                wrapClassName="data-contract-builder-modal"
                width={800}
                buttons={[]}
                title={titleText}
                style={modalStyle}
                bodyStyle={modalBodyStyle}
                onCancel={() => onCancel?.()}
            >
                <DataContractBuilder
                    entityUrn={entityUrn}
                    initialState={initialState}
                    onSubmit={onSubmit}
                    onCancel={onCancel}
                />
            </Modal>
            <ConfirmationModal
                isOpen={showConfirmationModal}
                handleClose={() => setShowConfirmationModal(false)}
                handleConfirm={() => onCancel?.()}
                modalTitle="Exit Editor"
                modalText="Are you sure you want to exit the editor? All changes will be lost"
            />
        </ClickOutside>
    );
};
