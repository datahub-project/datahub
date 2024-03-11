import React from 'react';
import { FormOutlined } from '@ant-design/icons';
import { IconItemTitle } from './IconItemTitle';
import { MenuItemStyle } from './styledComponent';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Edit View Menu Item
 */
export const EditViewItem = ({ key, onClick }: Props) => {
    return (
        <MenuItemStyle key={key} onClick={onClick} data-testid="view-dropdown-edit">
            <IconItemTitle tip="Edit this View" title="Edit" icon={<FormOutlined />} />
        </MenuItemStyle>
    );
};
