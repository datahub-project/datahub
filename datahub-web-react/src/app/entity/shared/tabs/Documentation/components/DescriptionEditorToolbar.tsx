import { CheckOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import TabToolbar from '../../../components/styled/TabToolbar';

type DescriptionEditorToolbarProps = {
    disableSave: boolean;
    onClose: () => void;
    onSave: () => void;
};

export const DescriptionEditorToolbar = ({ disableSave, onClose, onSave }: DescriptionEditorToolbarProps) => {
    return (
        <TabToolbar>
            <Button type="text" onClick={onClose}>
                Back
            </Button>
            <Button data-testid="description-editor-save-button" onClick={onSave} disabled={disableSave}>
                <CheckOutlined /> Save
            </Button>
        </TabToolbar>
    );
};
