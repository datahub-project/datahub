import React from 'react';
import { StopOutlined } from '@ant-design/icons';
import { Menu } from 'antd';
import { IconItemTitle } from './IconItemTitle';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Remove the User's default view item
 */
export const RemoveUserDefaultItem = ({ key, onClick }: Props) => {
    return (
        <Menu.Item key={key} onClick={onClick} data-testid="view-dropdown-remove-user-default">
            <IconItemTitle
                tip="Remova esta View como seu padrão pessoal."
                title="Remover como padrão"
                icon={<StopOutlined />}
            />
        </Menu.Item>
    );
};
