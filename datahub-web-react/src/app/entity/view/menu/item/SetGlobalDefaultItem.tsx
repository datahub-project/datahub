import React from 'react';
import { useTranslation } from 'react-i18next';

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
    const { t } = useTranslation('entity.views');
    return (
        <MenuItemStyle key={key} onClick={onClick} data-testid="view-dropdown-set-global-default">
            <IconItemTitle
                tip={t('menu.makeOrgDefaultTooltipLegacy')}
                title={t('menu.makeOrgDefault')}
                icon={<GlobalDefaultViewIcon />}
            />
        </MenuItemStyle>
    );
};
