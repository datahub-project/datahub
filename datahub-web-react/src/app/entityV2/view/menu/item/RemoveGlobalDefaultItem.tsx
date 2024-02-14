import React from 'react';
import { StopOutlined } from '@ant-design/icons';
import { Menu } from 'antd';
import { IconItemTitle } from './IconItemTitle';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Remove the Global View
 */
export const RemoveGlobalDefaultItem = ({ key, onClick }: Props) => {
    return (
        <Menu.Item key={key} onClick={onClick}>
            <IconItemTitle
                tip="Remove this View as your organization's default."
                title="Remove organization default"
                icon={<StopOutlined />}
            />
        </Menu.Item>
    );
};
