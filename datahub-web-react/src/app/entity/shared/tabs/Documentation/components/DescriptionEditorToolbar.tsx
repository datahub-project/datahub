import { CheckOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import TabToolbar from '../../../components/styled/TabToolbar';

type DescriptionEditorToolbarProps = {
    disableSave: boolean;
    onClose: () => void;
    onSave: () => void;
};

export const DescriptionEditorToolbar = ({ disableSave, onClose, onSave }: DescriptionEditorToolbarProps) => {
    const { t } = useTranslation();
    return (
        <TabToolbar>
            <Button type="text" onClick={onClose}>
                {t('common.back')}
            </Button>
            <Button data-testid="description-editor-save-button" onClick={onSave} disabled={disableSave}>
                <CheckOutlined /> {t('common.save')}
            </Button>
        </TabToolbar>
    );
};
