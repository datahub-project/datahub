import React, { useState } from 'react';

import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

type Props = {
    cancelModalVisible?: boolean;
    onDiscard?: () => void;
    onCancel?: () => void;
};

export const DiscardDescriptionModal = ({ cancelModalVisible, onDiscard, onCancel }: Props) => {
    return (
        <ConfirmationModal
            isOpen={!!cancelModalVisible}
            handleClose={() => {
                onCancel?.();
            }}
            handleConfirm={() => onDiscard?.()}
            modalTitle="Exit Editor"
            modalText="Are you sure you want to close the documentation editor? Any unsaved changes will be lost."
        />
    );
};
