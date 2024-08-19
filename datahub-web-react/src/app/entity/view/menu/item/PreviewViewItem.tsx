import React from 'react';
import { Menu } from 'antd';
import { EyeOutlined } from '@ant-design/icons';
import { IconItemTitle } from './IconItemTitle';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Preview View Menu Item
 */
export const PreviewViewItem = ({ key, onClick }: Props) => {
    return (
        <Menu.Item key={key} onClick={onClick}>
            <IconItemTitle tip="Veja a definição da View." title="Visualização" icon={<EyeOutlined />} />
        </Menu.Item>
    );
};
