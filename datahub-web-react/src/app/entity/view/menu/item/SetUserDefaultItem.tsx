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
                tip="Make this View your personal default. You will have this View applied automatically."
                title="Make my default"
                icon={<UserDefaultViewIcon />}
            />
        </Menu.Item>
    );
};
