import React from 'react';
import { FormOutlined } from '@ant-design/icons';
import { Menu } from 'antd';
import { IconItemTitle } from './IconItemTitle';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Edit View Menu Item
 */
export const EditViewItem = ({ key, onClick }: Props) => {
    return (
        <Menu.Item key={key} onClick={onClick} data-testid="view-dropdown-edit">
            <IconItemTitle tip="Edit this View" title="Edit" icon={<FormOutlined />} />
        </Menu.Item>
    );
};
