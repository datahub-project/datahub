import React from 'react';

import { IconItemTitle } from '@app/entity/view/menu/item/IconItemTitle';
import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';
import { UserDefaultViewIcon } from '@app/entity/view/shared/UserDefaultViewIcon';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Set the User's default view item
 */
export const SetUserDefaultItem = ({ key, onClick }: Props) => {
    return (
        <MenuItemStyle key={key} onClick={onClick} data-testid="view-dropdown-set-user-default">
            <IconItemTitle
                tip="Make this View your personal default. You will have this View applied automatically."
                title="Make my default"
                icon={<UserDefaultViewIcon />}
            />
        </MenuItemStyle>
    );
};
