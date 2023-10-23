import { CheckOutlined, MailOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { Button } from 'antd';
import React from 'react';
import TabToolbar from '../../../components/styled/TabToolbar';

const ProposeButton = styled(Button)`
    margin-right: 10px;
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
        <TabToolbar>
            <Button type="text" onClick={onClose}>
                Back
            </Button>
            <div>
                {showPropose && (
                    <ProposeButton data-testid="propose-description" onClick={onPropose} disabled={disableSave}>
                        <MailOutlined /> Propose
                    </ProposeButton>
                )}
                <Button data-testid="description-editor-save-button" onClick={onSave} disabled={disableSave}>
                    <CheckOutlined /> Save
                </Button>
            </div>
        </TabToolbar>
    );
};
