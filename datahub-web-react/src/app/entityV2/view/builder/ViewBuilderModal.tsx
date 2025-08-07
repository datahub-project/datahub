import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { ViewBuilderForm } from '@app/entityV2/view/builder/ViewBuilderForm';
import { ViewBuilderMode } from '@app/entityV2/view/builder/types';
import { DEFAULT_BUILDER_STATE, ViewBuilderState } from '@app/entityV2/view/types';
import ClickOutside from '@app/shared/ClickOutside';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { Button, Modal } from '@src/alchemy-components';

const SaveButtonContainer = styled.div`
    width: 100%;
    display: flex;
    justify-content: right;
`;

const CancelButton = styled(Button)`
    margin-right: 12px;
`;

type Props = {
    mode: ViewBuilderMode;
    urn?: string;
    initialState?: ViewBuilderState;
    onSubmit: (input: ViewBuilderState) => void;
    onCancel?: () => void;
};

const getTitleText = (mode, urn) => {
    if (mode === ViewBuilderMode.PREVIEW) {
        return 'Preview View';
    }
    return urn !== undefined ? 'Edit View' : 'Create new View';
};

export const ViewBuilderModal = ({ mode, urn, initialState, onSubmit, onCancel }: Props) => {
    const [viewBuilderState, setViewBuilderState] = useState<ViewBuilderState>(initialState || DEFAULT_BUILDER_STATE);
    const [showConfirmationModal, setShowConfirmationModal] = useState(false);

    useEffect(() => {
        setViewBuilderState(initialState || DEFAULT_BUILDER_STATE);
    }, [initialState]);

    const canSave = viewBuilderState.name && viewBuilderState.viewType && viewBuilderState?.definition?.filter;
    const titleText = getTitleText(mode, urn);

    return (
        <ClickOutside onClickOutside={() => setShowConfirmationModal(true)} wrapperClassName="test-builder-modal">
            <Modal
                wrapClassName="view-builder-modal"
                buttons={[]}
                title={titleText}
                onCancel={() => onCancel?.()}
                data-testid="view-modal"
            >
                <ViewBuilderForm urn={urn} mode={mode} state={viewBuilderState} updateState={setViewBuilderState} />
                <SaveButtonContainer>
                    <CancelButton variant="text" color="gray" data-testid="view-builder-cancel" onClick={onCancel}>
                        Cancel
                    </CancelButton>
                    {mode === ViewBuilderMode.EDITOR && (
                        <Button
                            data-testid="view-builder-save"
                            disabled={!canSave}
                            onClick={() => onSubmit(viewBuilderState)}
                        >
                            Save
                        </Button>
                    )}
                </SaveButtonContainer>
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
                modalText="Are you sure you want to exit policy editor? All changes will be lost"
                confirmButtonText="Yes"
            />
        </ClickOutside>
    );
};
