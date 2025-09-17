import React, { useState } from 'react';
import styled from 'styled-components';

import { SearchSelect } from '@app/entity/shared/components/styled/search/SearchSelect';
import { EntityAndType } from '@app/entity/shared/types';
import ClickOutside from '@app/shared/ClickOutside';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { Modal } from '@src/alchemy-components';

import { EntityType } from '@types';

const StyledModal = styled(Modal)`
    top: 30px;
`;

const MODAL_WIDTH_PX = 800;

const MODAL_BODY_STYLE = { padding: 0, height: '70vh' };

type Props = {
    fixedEntityTypes?: Array<EntityType> | null;
    placeholderText?: string | null;
    titleText?: string | null;
    continueText?: string | null;
    onContinue: (entityUrns: string[]) => void;
    onCancel?: () => void;
    singleSelect?: boolean;
    hideToolbar?: boolean;
};

/**
 * Modal that can be used for searching & selecting a subset of the entities in the Metadata Graph in order to take a specific action.
 *
 * This component provides easy ways to filter for a specific set of entity types, and provides a set of entity urns
 * when the selection is complete.
 */
export const SearchSelectModal = ({
    fixedEntityTypes,
    placeholderText,
    titleText,
    continueText,
    onContinue,
    onCancel,
    singleSelect,
    hideToolbar,
}: Props) => {
    const [selectedEntities, setSelectedEntities] = useState<EntityAndType[]>([]);
    const [showConfirmationModal, setShowConfirmationModal] = useState(false);

    const onCancelSelect = () => {
        if (selectedEntities.length > 0) {
            setShowConfirmationModal(true);
        } else {
            onCancel?.();
        }
    };

    return (
        <ClickOutside onClickOutside={onCancelSelect} wrapperClassName="search-select-modal">
            <StyledModal
                wrapClassName="search-select-modal"
                bodyStyle={MODAL_BODY_STYLE}
                title={titleText || 'Select entities'}
                width={MODAL_WIDTH_PX}
                open
                onCancel={onCancelSelect}
                buttons={[
                    {
                        text: 'Cancel',
                        variant: 'text',
                        onClick: () => onCancel?.(),
                    },
                    {
                        text: continueText || 'Done',
                        onClick: () => onContinue(selectedEntities.map((entity) => entity.urn)),
                        variant: 'filled',
                        disabled: selectedEntities.length === 0,
                        buttonDataTestId: 'search-select-continue-button',
                        id: 'continueButton',
                    },
                ]}
            >
                <SearchSelect
                    fixedEntityTypes={fixedEntityTypes}
                    placeholderText={placeholderText}
                    selectedEntities={selectedEntities}
                    setSelectedEntities={setSelectedEntities}
                    singleSelect={singleSelect}
                    hideToolbar={hideToolbar}
                />
            </StyledModal>
            <ConfirmationModal
                isOpen={showConfirmationModal}
                handleClose={() => setShowConfirmationModal(false)}
                handleConfirm={() => onCancel?.()}
                modalTitle="Exit Selection"
                modalText={`Are you sure you want to exit? ${selectedEntities.length} selection(s) will be cleared.`}
            />
        </ClickOutside>
    );
};
