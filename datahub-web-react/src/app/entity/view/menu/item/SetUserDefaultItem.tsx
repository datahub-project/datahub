import React from 'react';
import { useTranslation } from 'react-i18next';

import { IconItemTitle } from '@app/entity/view/menu/item/IconItemTitle';
import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';
import { UserDefaultViewIcon } from '@app/entity/view/shared/UserDefaultViewIcon';

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
