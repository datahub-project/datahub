import styled from 'styled-components';
import { Button } from 'antd';
import React from 'react';
import TabToolbar from '../../../components/styled/TabToolbar';
import { REDESIGN_COLORS, ANTD_GRAY } from '../../../constants';

const StyledTabToolbar = styled(TabToolbar)`
    justify-content: flex-end !important;
`;

const SaveButton = styled(Button)`
    background-color: #5c3fd1;
    color: ${ANTD_GRAY[1]};
    box-shadow: none;
    border-color: ${REDESIGN_COLORS.DARK_PURPLE};
    line-height: 0;

    &:hover,
    &:focus {
        transition: 0.15s;
        opacity: 0.9;
        border-color: ${REDESIGN_COLORS.DARK_PURPLE};
        background-color: ${REDESIGN_COLORS.HOVER_PURPLE};
        color: ${ANTD_GRAY[1]};
    }
`;

const StyledButton = styled(Button)`
    margin-right: 10px;
    box-shadow: none;
    color: ${REDESIGN_COLORS.DARK_PURPLE};
    border-color: ${REDESIGN_COLORS.DARK_PURPLE};
    line-height: 0;

    &:hover,
    &:focus {
        background-color: ${REDESIGN_COLORS.DARK_PURPLE};
        color: #fff;
        border-color: ${REDESIGN_COLORS.DARK_PURPLE};
    }
`;

type DescriptionEditorToolbarProps = {
    disableSave: boolean;
    onSave: () => void;
    onCancel: () => void;
};

export const DescriptionEditorToolbar = ({ disableSave, onSave, onCancel }: DescriptionEditorToolbarProps) => {
    return (
        <StyledTabToolbar>
            <StyledButton data-testid="description-editor-cancel-button" onClick={onCancel}>
                Cancel
            </StyledButton>
            <SaveButton data-testid="description-editor-save-button" onClick={onSave} disabled={disableSave}>
                Publish
            </SaveButton>
        </StyledTabToolbar>
    );
};
