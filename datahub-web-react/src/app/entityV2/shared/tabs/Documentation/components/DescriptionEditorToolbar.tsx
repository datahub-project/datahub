import styled from 'styled-components';
import React from 'react';
import { Button } from '@src/alchemy-components';
import TabToolbar from '../../../components/styled/TabToolbar';

const StyledTabToolbar = styled(TabToolbar)`
    justify-content: flex-end !important;
    gap: 16px;
`;

type DescriptionEditorToolbarProps = {
    disableSave: boolean;
    onSave: () => void;
    onPropose: () => void;
    onCancel: () => void;
    showPropose: boolean;
};

export const DescriptionEditorToolbar = ({
    disableSave,
    onSave,
    onPropose,
    onCancel,
    showPropose,
}: DescriptionEditorToolbarProps) => {
    return (
        <StyledTabToolbar>
            <Button variant="text" color="gray" data-testid="description-editor-cancel-button" onClick={onCancel}>
                Cancel
<<<<<<< HEAD
            </StyledButton>
            {showPropose && (
                <StyledButton data-testid="propose-description" onClick={onPropose} disabled={disableSave}>
                    Propose
                </StyledButton>
            )}
            <SaveButton data-testid="description-editor-save-button" onClick={onSave} disabled={disableSave}>
||||||| f14c42d2ef7
            </StyledButton>
            <SaveButton data-testid="description-editor-save-button" onClick={onSave} disabled={disableSave}>
=======
            </Button>
            <Button data-testid="description-editor-save-button" onClick={onSave} disabled={disableSave}>
>>>>>>> master
                Publish
            </Button>
        </StyledTabToolbar>
    );
};
