import { StopOutlined } from '@ant-design/icons';
import React from 'react';

import { IconItemTitle } from '@app/entity/view/menu/item/IconItemTitle';
import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Remove the Global View
 */
export const RemoveGlobalDefaultItem = ({ key, onClick }: Props) => {
    return (
        <MenuItemStyle key={key} onClick={onClick}>
            <IconItemTitle
                tip="Remove this View as your organization's default."
                title="Remove organization default"
                icon={<StopOutlined />}
            />
        </MenuItemStyle>
    );
};
