import React from 'react';
import styled from 'styled-components';

import TabToolbar from '@app/entityV2/shared/components/styled/TabToolbar';
import { Button } from '@src/alchemy-components';

const StyledTabToolbar = styled(TabToolbar)`
    justify-content: flex-end !important;
    gap: 16px;
`;

type DescriptionEditorToolbarProps = {
    disableSave: boolean;
    onSave: () => void;
    onPropose: () => void;
    onCancel: () => void;
    disablePropose: boolean;
};

export const DescriptionEditorToolbar = ({
    disableSave,
    onSave,
    onPropose,
    onCancel,
    disablePropose,
}: DescriptionEditorToolbarProps) => {
    return (
        <StyledTabToolbar>
            <Button
                variant="text"
                type="button"
                color="gray"
                data-testid="description-editor-cancel-button"
                onClick={onCancel}
            >
                Cancel
            </Button>
            <Button
                variant="outline"
                type="button"
                data-testid="propose-description"
                onClick={onPropose}
                disabled={disablePropose}
            >
                Propose
            </Button>

            <Button data-testid="description-editor-save-button" onClick={onSave} disabled={disableSave}>
                Publish
            </Button>
        </StyledTabToolbar>
    );
};
