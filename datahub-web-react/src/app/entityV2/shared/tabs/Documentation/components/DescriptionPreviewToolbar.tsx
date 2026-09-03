import { EditOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

import TabToolbar from '@app/entityV2/shared/components/styled/TabToolbar';

type DescriptionPreviewToolbarProps = {
    onEdit: () => void;
};

export const DescriptionPreviewToolbar = ({ onEdit }: DescriptionPreviewToolbarProps) => {
    const { t: tc } = useTranslation('common.actions');
    return (
        <TabToolbar>
            <Button type="text" onClick={onEdit}>
                <EditOutlined /> {tc('edit')}
            </Button>
        </TabToolbar>
    );
};
