import { Modal } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

import { DataContractBuilder } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/DataContractBuilder';
import { DataContractBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/contract/builder/types';
import ClickOutside from '@app/shared/ClickOutside';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

import { DataContract } from '@types';

const modalStyle = {};
const modalBodyStyle = {
    paddingRight: 0,
    paddingLeft: 0,
    paddingBottom: 0,
    paddingTop: 0,
    height: '70vh',
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
    const { t } = useTranslation('entity.profile.validations');
    const isEditing = initialState !== undefined;
    const titleText = isEditing ? t('contractBuilder.titleEdit') : t('contractBuilder.newContractTitle');

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
                modalTitle={t('contractBuilder.exitEditor')}
                modalText={t('contractBuilder.areYouSureExit')}
            />
        </ClickOutside>
    );
};
