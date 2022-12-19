import React from 'react';
import { DeleteOutlined } from '@ant-design/icons';
import { Menu } from 'antd';
import { IconItemTitle } from './IconItemTitle';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Delete a View Item
 */
export const DeleteViewItem = ({ key, onClick }: Props) => {
    return (
        <Menu.Item key={key} onClick={onClick} data-testid="view-dropdown-delete">
            <IconItemTitle tip="Delete this View" title="Delete" icon={<DeleteOutlined />} />
        </Menu.Item>
    );
};
