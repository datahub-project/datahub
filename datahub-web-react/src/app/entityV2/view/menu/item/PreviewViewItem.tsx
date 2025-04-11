import React from 'react';
import { EyeOutlined } from '@ant-design/icons';
import { ViewItem } from './ViewItem';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Preview View Menu Item
 */
export const PreviewViewItem = ({ key, onClick }: Props) => {
    return (
        <ViewItem key={key} onClick={onClick} tip="See the View definition." title="Preview" icon={<EyeOutlined />} />
    );
};
