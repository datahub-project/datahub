/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { DeleteOutlined } from '@ant-design/icons';
import React from 'react';

import { IconItemTitle } from '@app/entity/view/menu/item/IconItemTitle';
import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';

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
