import { CheckOutlined, MailOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { Button } from 'antd';
import React from 'react';
import TabToolbar from '../../../components/styled/TabToolbar';

const StyledTabToolbar = styled(TabToolbar)`
    justify-content: flex-end !important;
`;

const ProposeButton = styled(Button)`
    margin-right: 10px;
    margin-left: 10px;
    background-color: #6c6b88;
    color: #fff;
    box-shadow: none;
    border-color: #6c6b88;
    line-height: 0;
    &:hover,
    &:focus {
        background-color: #fff;
        color: #6c6b88;
        border-color: #6c6b88;
    }
`;

const StyledButton = styled(Button)`
    box-shadow: none;
    color: #6c6b88;
    border-color: #6c6b88;
    line-height: 0;
    &:hover,
    &:focus {
        background-color: #6c6b88;
        color: #fff;
        border-color: #6c6b88;
    }
`;

type DescriptionEditorToolbarProps = {
    disableSave: boolean;
    onClose: () => void;
    onSave: () => void;
    onPropose: () => void;
    showPropose: boolean;
};

export const DescriptionEditorToolbar = ({
    disableSave,
    onClose,
    onSave,
    onPropose,
    showPropose,
}: DescriptionEditorToolbarProps) => {
    return (
        <StyledTabToolbar>
            <StyledButton data-testid="description-editor-save-button" onClick={onSave} disabled={disableSave}>
                Save
            </StyledButton>
            {showPropose && (
                <ProposeButton data-testid="propose-description" onClick={onPropose} disabled={disableSave}>
                    Propose
                </ProposeButton>
            )}
        </StyledTabToolbar>
    );
};
