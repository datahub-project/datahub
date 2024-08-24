import React from 'react';
import { Menu } from 'antd';
import { UserDefaultViewIcon } from '../../shared/UserDefaultViewIcon';
import { IconItemTitle } from './IconItemTitle';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Set the User's default view item
 */
export const SetUserDefaultItem = ({ key, onClick }: Props) => {
    return (
        <Menu.Item key={key} onClick={onClick} data-testid="view-dropdown-set-user-default">
            <IconItemTitle
                tip="Faça desta Visualização seu padrão pessoal. Você terá esta visualização aplicada automaticamente."
                title="Tornar meu padrão"
                icon={<UserDefaultViewIcon />}
            />
        </Menu.Item>
    );
};
