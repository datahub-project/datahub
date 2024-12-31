import { Modal } from 'antd';
import React from 'react';
import FormSelector from './FormSelector';

interface Props {
    isFormSelectionModalVisible: boolean;
    hideFormSelectionModal: () => void;
    selectFormUrn: (urn: string) => void;
}

export default function FormSelectionModal({
    isFormSelectionModalVisible,
    hideFormSelectionModal,
    selectFormUrn,
}: Props) {
    return (
        <Modal open={isFormSelectionModalVisible} onCancel={hideFormSelectionModal} footer={null}>
            <FormSelector selectFormUrn={selectFormUrn} />
        </Modal>
    );
}
