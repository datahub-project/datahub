import { EditOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import React from 'react';

import TabToolbar from '@app/entity/shared/components/styled/TabToolbar';

type DescriptionPreviewToolbarProps = {
    onEdit: () => void;
};

export const DescriptionPreviewToolbar = ({ onEdit }: DescriptionPreviewToolbarProps) => {
    return (
        <TabToolbar>
            <Button type="text" onClick={onEdit}>
                <EditOutlined /> Edit
            </Button>
        </TabToolbar>
    );
};
