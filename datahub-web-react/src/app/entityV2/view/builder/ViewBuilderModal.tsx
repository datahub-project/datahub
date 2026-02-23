import React, { useEffect, useMemo, useState } from 'react';

import { ViewBuilderForm } from '@app/entityV2/view/builder/ViewBuilderForm';
import { ViewBuilderMode } from '@app/entityV2/view/builder/types';
import { DEFAULT_BUILDER_STATE, ViewBuilderState } from '@app/entityV2/view/types';
import ClickOutside from '@app/shared/ClickOutside';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { Modal } from '@src/alchemy-components';
import { ModalButton } from '@src/alchemy-components/components/Modal/Modal';

type Props = {
    mode: ViewBuilderMode;
    urn?: string;
    initialState?: ViewBuilderState;
    onSubmit: (input: ViewBuilderState) => void;
    onCancel?: () => void;
};

const getTitleText = (mode: ViewBuilderMode, urn?: string): string => {
    if (mode === ViewBuilderMode.PREVIEW) {
        return 'Preview View';
    }
    return urn !== undefined ? 'Edit View' : 'Create New View';
};

const MODAL_WIDTH = '60%';

export const ViewBuilderModal = ({ mode, urn, initialState, onSubmit, onCancel }: Props) => {
    const [viewBuilderState, setViewBuilderState] = useState<ViewBuilderState>(initialState || DEFAULT_BUILDER_STATE);
    const [showConfirmationModal, setShowConfirmationModal] = useState(false);

    useEffect(() => {
        setViewBuilderState(initialState || DEFAULT_BUILDER_STATE);
    }, [initialState]);

    const hasFilters = (viewBuilderState?.definition?.filter?.filters?.length ?? 0) > 0;
    const canSave = viewBuilderState.name && viewBuilderState.viewType && hasFilters;
    const titleText = getTitleText(mode, urn);

    const footerButtons: ModalButton[] = useMemo(() => {
        const buttons: ModalButton[] = [
            {
                text: 'Cancel',
                variant: 'text',
                color: 'gray',
                onClick: () => onCancel?.(),
                buttonDataTestId: 'view-builder-cancel',
            },
        ];

        if (mode === ViewBuilderMode.EDITOR) {
            buttons.push({
                text: 'Save',
                onClick: () => onSubmit(viewBuilderState),
                disabled: !canSave,
                buttonDataTestId: 'view-builder-save',
            });
        }

        return buttons;
    }, [mode, onCancel, onSubmit, viewBuilderState, canSave]);

    return (
        <ClickOutside onClickOutside={() => setShowConfirmationModal(true)} wrapperClassName="test-builder-modal">
            <Modal
                wrapClassName="view-builder-modal"
                wrapProps={{ style: { overflow: 'hidden' } }}
                bodyStyle={{ overflow: 'hidden', maxHeight: '75vh' }}
                buttons={footerButtons}
                title={titleText}
                subtitle="Views control which assets are visible when applied. Define filters to scope search results, recommendations, and browsing to only the assets that match."
                onCancel={() => onCancel?.()}
                data-testid="view-modal"
                width={MODAL_WIDTH}
            >
                <ViewBuilderForm urn={urn} mode={mode} state={viewBuilderState} updateState={setViewBuilderState} />
            </Modal>
            <ConfirmationModal
                isOpen={showConfirmationModal}
                handleClose={() => {
                    setShowConfirmationModal(false);
                }}
                handleConfirm={() => {
                    setShowConfirmationModal(false);
                    onCancel?.();
                }}
                modalTitle="Exit View Editor"
                modalText="Are you sure you want to exit the View editor? All changes will be lost."
                confirmButtonText="Yes"
            />
        </ClickOutside>
    );
};
