import React from 'react';
import { Menu } from 'antd';
import { GlobalDefaultViewIcon } from '../../shared/GlobalDefaultViewIcon';
import { IconItemTitle } from './IconItemTitle';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Set the Global Default Item
 */
export const SetGlobalDefaultItem = ({ key, onClick }: Props) => {
    return (
        <Menu.Item key={key} onClick={onClick} data-testid="view-dropdown-set-global-default">
            <IconItemTitle
                tip="Torne esta View o padrão da sua organização. Todos os novos usuários terão esta Visualização aplicada automaticamente."
                title="Tornar a organização padrão"
                icon={<GlobalDefaultViewIcon />}
            />
        </Menu.Item>
    );
};
