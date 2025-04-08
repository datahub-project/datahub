import { EyeOutlined } from '@ant-design/icons';
import React from 'react';

import { IconItemTitle } from '@app/entity/view/menu/item/IconItemTitle';
import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Preview View Menu Item
 */
export const PreviewViewItem = ({ key, onClick }: Props) => {
    return (
        <MenuItemStyle key={key} onClick={onClick}>
            <IconItemTitle tip="See the View definition." title="Preview" icon={<EyeOutlined />} />
        </MenuItemStyle>
    );
};
