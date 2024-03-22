import React from 'react';
import { StopOutlined } from '@ant-design/icons';
import { IconItemTitle } from './IconItemTitle';
import { MenuItemStyle } from './styledComponent';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Remove the User's default view item
 */
export const RemoveUserDefaultItem = ({ key, onClick }: Props) => {
    return (
        <MenuItemStyle key={key} onClick={onClick} data-testid="view-dropdown-remove-user-default">
            <IconItemTitle
                tip="Remove this View as your personal default."
                title="Remove as default"
                icon={<StopOutlined />}
            />
        </MenuItemStyle>
    );
};
