import React from 'react';
import { DeleteOutlined } from '@ant-design/icons';
import { IconItemTitle } from './IconItemTitle';
import { MenuItemStyle } from './styledComponent';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Delete a View Item
 */
export const DeleteViewItem = ({ key, onClick }: Props) => {
    return (
        <MenuItemStyle key={key} onClick={onClick} data-testid="view-dropdown-delete">
            <IconItemTitle tip="Delete this View" title="Delete" icon={<DeleteOutlined />} />
        </MenuItemStyle>
    );
};
