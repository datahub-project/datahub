/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { IconItemTitle } from '@app/entity/view/menu/item/IconItemTitle';
import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';
import { GlobalDefaultViewIcon } from '@app/entity/view/shared/GlobalDefaultViewIcon';

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
