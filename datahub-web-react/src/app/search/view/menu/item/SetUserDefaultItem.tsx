import React from 'react';
import { useTranslation } from 'react-i18next';

import { MenuItemStyle } from '@app/entityV2/view/menu/item/styledComponent';
import { IconItemTitle } from '@app/search/view/menu/item/IconItemTitle';
import { UserDefaultViewIcon } from '@app/search/view/shared/UserDefaultViewIcon';

type Props = {
    key: string;
    onClick: () => void;
};

/**
 * Set the User's default view item
 */
export const SetUserDefaultItem = ({ key, onClick }: Props) => {
    const { t } = useTranslation('entity.views');
    return (
        <MenuItemStyle key={key} onClick={onClick} data-testid="view-dropdown-set-user-default">
            <IconItemTitle
                tip={t('menu.makeDefaultTooltipLegacy')}
                title={t('menu.makeDefault')}
                icon={<UserDefaultViewIcon />}
            />
        </MenuItemStyle>
    );
};
