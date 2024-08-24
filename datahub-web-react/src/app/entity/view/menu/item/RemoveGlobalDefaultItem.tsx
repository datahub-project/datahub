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
                tip="Remova esta View como padrão da sua organização."
                title="Remover padrão da organização"
                icon={<StopOutlined />}
            />
        </Menu.Item>
    );
};
