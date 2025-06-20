import { Button, Modal } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { EntityAndType } from '@app/entity/shared/types';
import { SearchSelect } from '@app/entityV2/shared/components/styled/search/SearchSelect';
import ClickOutside from '@app/shared/ClickOutside';

import { EntityType } from '@types';

const StyledModal = styled(Modal)`
    top: 30px;
`;

const MODAL_WIDTH_PX = 800;

const UI_Z_INDEX = 1000;

const MODAL_BODY_STYLE = { padding: 0, height: '70vh' };

type SearchSelectModalProps = {
    fixedEntityTypes?: Array<EntityType> | null;
    placeholderText?: string | null;
    titleText?: string | null;
    continueText?: string | null;
    onContinue: (entityUrns: string[]) => void;
    onCancel?: () => void;
    limit?: number;
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
    limit,
}: SearchSelectModalProps) => {
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
                zIndex={UI_Z_INDEX}
                visible
                onCancel={onCancelSelect}
                footer={
                    <>
                        <Button onClick={onCancel} type="text">
                            Cancel
                        </Button>
                        <Button
                            type="primary"
                            id="continueButton"
                            onClick={() => onContinue(selectedEntities.map((entity) => entity.urn))}
                            disabled={selectedEntities.length === 0}
                        >
                            {continueText || 'Done'}
                        </Button>
                    </>
                }
            >
                <SearchSelect
                    fixedEntityTypes={fixedEntityTypes}
                    placeholderText={placeholderText}
                    selectedEntities={selectedEntities}
                    setSelectedEntities={setSelectedEntities}
                    limit={limit}
                />
            </StyledModal>
        </ClickOutside>
    );
};
