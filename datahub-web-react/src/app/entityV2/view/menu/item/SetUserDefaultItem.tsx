/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { ViewItem } from '@app/entityV2/view/menu/item/ViewItem';
import { UserDefaultViewIcon } from '@app/entityV2/view/shared/UserDefaultViewIcon';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { useCustomTheme } from '@src/customThemeContext';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Set the User's default view item
 */
export const SetUserDefaultItem = ({ key, onClick }: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const { theme } = useCustomTheme();

    return (
        <ViewItem
            key={key}
            onClick={onClick}
            dataTestId="view-dropdown-set-user-default"
            tip="Make this View your personal default. You will have this View applied automatically."
            title="Make my default"
            icon={
                <UserDefaultViewIcon
                    color={isShowNavBarRedesign ? theme?.styles['primary-color'] : REDESIGN_COLORS.TERTIARY_GREEN}
                />
            }
        />
    );
};
