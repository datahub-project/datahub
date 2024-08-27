import React from 'react';
import { EditOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import { useTranslation } from 'react-i18next';
import TabToolbar from '../../../components/styled/TabToolbar';

type DescriptionPreviewToolbarProps = {
    onEdit: () => void;
};

export const DescriptionPreviewToolbar = ({ onEdit }: DescriptionPreviewToolbarProps) => {
    const { t } = useTranslation();
    return (
        <TabToolbar>
            <Button type="text" onClick={onEdit}>
                <EditOutlined /> {t('common.edit')}
            </Button>
        </TabToolbar>
    );
};
