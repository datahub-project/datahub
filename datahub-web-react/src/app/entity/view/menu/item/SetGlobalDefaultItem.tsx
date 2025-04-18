import React from 'react';
import { GlobalDefaultViewIcon } from '../../shared/GlobalDefaultViewIcon';
import { IconItemTitle } from './IconItemTitle';
import { MenuItemStyle } from './styledComponent';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Set the Global Default Item
 */
export const SetGlobalDefaultItem = ({ key, onClick }: Props) => {
    return (
        <MenuItemStyle key={key} onClick={onClick} data-testid="view-dropdown-set-global-default">
            <IconItemTitle
                tip="Make this View your organization's default. All new users will have this View applied automatically."
                title="Make organization default"
                icon={<GlobalDefaultViewIcon />}
            />
        </MenuItemStyle>
    );
};
