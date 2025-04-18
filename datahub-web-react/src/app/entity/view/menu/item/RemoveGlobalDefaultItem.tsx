import React from 'react';
import { StopOutlined } from '@ant-design/icons';
import { IconItemTitle } from './IconItemTitle';
import { MenuItemStyle } from './styledComponent';

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
