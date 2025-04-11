import { Modal } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { Button } from '@src/alchemy-components';
import { ModalButtonContainer } from '@src/app/shared/button/styledComponents';
import { EntityType } from '../../../../../../types.generated';
import ClickOutside from '../../../../../shared/ClickOutside';
import { EntityAndType } from '../../../types';
import { SearchSelect } from './SearchSelect';

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

    const onCancelSelect = () => {
        if (selectedEntities.length > 0) {
            Modal.confirm({
                title: `Exit Selection`,
                content: `Are you sure you want to exit? ${selectedEntities.length} selection(s) will be cleared.`,
                onOk() {
                    onCancel?.();
                },
                onCancel() {},
                okText: 'Yes',
                maskClosable: true,
                closable: true,
            });
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
                footer={
                    <ModalButtonContainer>
                        <Button onClick={onCancel} variant="text" color="gray">
                            Cancel
                        </Button>
                        <Button
                            id="continueButton"
                            data-testid="search-select-continue-button"
                            onClick={() => onContinue(selectedEntities.map((entity) => entity.urn))}
                            disabled={selectedEntities.length === 0}
                        >
                            {continueText || 'Done'}
                        </Button>
                    </ModalButtonContainer>
                }
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
        </ClickOutside>
    );
};
