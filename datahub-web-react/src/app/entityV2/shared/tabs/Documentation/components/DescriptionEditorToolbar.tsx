import React from 'react';
import { useTranslation } from 'react-i18next';
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
    onCancel: () => void;
};

export const DescriptionEditorToolbar = ({ disableSave, onSave, onCancel }: DescriptionEditorToolbarProps) => {
    const { t: tc } = useTranslation('common.actions');
    return (
        <StyledTabToolbar>
            <Button variant="text" color="gray" data-testid="description-editor-cancel-button" onClick={onCancel}>
                {tc('cancel')}
            </Button>
            <Button data-testid="description-editor-save-button" onClick={onSave} disabled={disableSave}>
                {tc('publish')}
            </Button>
        </StyledTabToolbar>
    );
};
